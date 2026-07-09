#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
task_fsm_node.py

任务：
    多机协同起飞 + 主从跟随 + 人工阶段指令 + 松钩/收绳 + 同步降落 + 紧急保持。

设计边界：
    1. 本文件只负责任务状态机。
    2. 不处理 PREPARE / COMMIT / ACK，这些属于 sync_gate_node。
    3. 不直接调用 sync_gate_node 对象。
    4. 只通过本机 service 和 sync_gate_node 交互。
    5. 不是所有状态切换都需要同步器。
       只有“多机必须同时进入”的动作才通过同步器。

本机同步接口：
    task_fsm_node -> sync_gate_node:
        /<self_id>/task/sync_status
        /<self_id>/task/sync_request

    sync_gate_node -> task_fsm_node:
        /<self_id>/task/sync_event

    上面三个接口均使用 aofe_star/JsonPayload.srv，payload 是 JSON 字符串。

外部模块接口：
    1. 轨迹规划器：
        /<self_id>/planner/request

    2. 控制器：
        /<self_id>/controller/command

    3. 松钩/收绳执行器：
        /<self_id>/hook/command

    4. 状态发布：
        /<self_id>/mission/state

    5. 人工指令输入：
        /operator/takeoff
        /operator/enter_follow
        /operator/hook_sequence
        /operator/land
        /operator/emergency_hold
"""

import json
import math
import queue
import threading
from typing import Any, Dict, Optional, Callable

import rospy
from std_msgs.msg import String, Bool
from sensor_msgs.msg import NavSatFix
from mavros_msgs.msg import State, RCIn, ManualControl, ExtendedState
from std_srvs.srv import Trigger, TriggerResponse
from aofe_star.srv import JsonPayload, JsonPayloadResponse

try:
    from mavros_msgs.msg import EstimatorStatus
except ImportError:
    EstimatorStatus = None

from mavros_msgs.srv import CommandBool, SetMode

# ============================================================
# sync_gate_node -> task_fsm_node 的事件类型
# ============================================================

EVENT_REQUEST_ACCEPTED = "REQUEST_ACCEPTED"
EVENT_REQUEST_REJECTED = "REQUEST_REJECTED"
EVENT_SCHEDULED = "SCHEDULED"
EVENT_START = "START"
EVENT_ABORT = "ABORT"


def now_sec() -> float:
    return rospy.Time.now().to_sec()


def safe_json_loads(text: str) -> Optional[dict]:
    try:
        return json.loads(text)
    except Exception:
        return None


def get_bool_param(name: str, default: bool) -> bool:
    value = rospy.get_param(name, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in ["1", "true", "yes", "on"]


class TaskFSMNode:
    """
    实际任务 FSM 流程：

        TASK_IDLE
            ↓ master 收到 start / slave 自动进入
        TASK_SELF_CHECK
            ↓ 自检通过
        TASK_TAKEOFF_READY
            ↓ master 收到 /operator/takeoff 后，通过同步器 action=takeoff
        TASK_TAKEOFF_RUNNING
            ↓ 到达指定高度
        TASK_WAIT_ENTER_FOLLOW_CMD
            ↓ 收到 /operator/enter_follow，不需要同步器
        TASK_MASTER_PILOT_HOLD       master：等待飞手接管/保持
        TASK_FOLLOW_MASTER           slave：继续跟随 master

            ↓ 收到 /operator/hook_sequence，不需要同步器
        TASK_HOOK_SEQUENCE_RUNNING
            ↓ 松钩，5s 后收绳，再回到主从飞行阶段

            ↓ master 收到 /operator/land，通过同步器 action=land
        TASK_LAND_RUNNING
            ↓ 落地完成
        TASK_RESETTING
            ↓ 复位等待下一轮
        TASK_IDLE / TASK_SELF_CHECK

    任意状态收到 /operator/emergency_hold：
        TASK_EMERGENCY_HOLD
    """

    # ============================================================
    # 任务状态
    # ============================================================

    TASK_IDLE = "TASK_IDLE"

    TASK_SELF_CHECK = "TASK_SELF_CHECK"

    # 自检通过、等待同步起飞。
    # slave 在这个状态下对同步器报告 expected_action="takeoff"。
    # master 在这个状态下，只有收到人工起飞指令后才 request_sync("takeoff")。
    TASK_TAKEOFF_READY = "TASK_TAKEOFF_READY"

    TASK_TAKEOFF_RUNNING = "TASK_TAKEOFF_RUNNING"

    # 到达指定高度后，等待人工指令进入“主机人工控制/从机跟随”阶段。
    TASK_WAIT_ENTER_FOLLOW_CMD = "TASK_WAIT_ENTER_FOLLOW_CMD"

    # master：进入飞手接管前的 hold/等待状态。
    # 注意：这里默认是“软件 hold”，不是 PX4 HOLD。
    TASK_MASTER_PILOT_HOLD = "TASK_MASTER_PILOT_HOLD"

    # slave：从机跟随 master。
    TASK_FOLLOW_MASTER = "TASK_FOLLOW_MASTER"

    # 松钩、延迟 5s、收绳。
    # 这个阶段飞行控制权保持不变：
    #   master 仍然由飞手/hold 控制；
    #   slave 仍然跟随 master。
    TASK_HOOK_SEQUENCE_RUNNING = "TASK_HOOK_SEQUENCE_RUNNING"

    # 同步 LAND 后执行降落。
    TASK_LAND_RUNNING = "TASK_LAND_RUNNING"

    # 落地后复位。
    TASK_RESETTING = "TASK_RESETTING"

    # 紧急软件保持。
    TASK_EMERGENCY_HOLD = "TASK_EMERGENCY_HOLD"

    TASK_ABORTED = "TASK_ABORTED"

    def __init__(self):
        # ========================================================
        # 基本身份参数
        # ========================================================

        self.role = rospy.get_param("~role", "slave").lower().strip()
        self.self_id = rospy.get_param("~self_id", "uav1")
        self.participants = self._parse_csv_param(rospy.get_param("~participants", ""))

        if self.role not in ["master", "slave"]:
            raise RuntimeError("~role must be master or slave")

        # ========================================================
        # 本机同步接口 service
        # 参数名保留 *_topic，是为了兼容已有 launch 文件。
        # ========================================================

        self.task_status_topic = rospy.get_param(
            "~task_status_topic",
            f"/{self.self_id}/task/sync_status",
        )
        self.task_request_topic = rospy.get_param(
            "~task_request_topic",
            f"/{self.self_id}/task/sync_request",
        )
        self.task_event_topic = rospy.get_param(
            "~task_event_topic",
            f"/{self.self_id}/task/sync_event",
        )

        # 外部启动，仅 master 使用。
        self.start_topic = rospy.get_param(
            "~start_topic",
            f"/{self.self_id}/task/start",
        )

        # ========================================================
        # 任务模块接口 topic
        # ========================================================

        self.mission_state_topic = rospy.get_param(
            "~mission_state_topic",
            f"/{self.self_id}/mission/state",
        )

        self.planner_request_topic = rospy.get_param(
            "~planner_request_topic",
            f"/{self.self_id}/planner/request",
        )

        self.controller_command_topic = rospy.get_param(
            "~controller_command_topic",
            f"/{self.self_id}/controller/command",
        )

        self.hook_command_topic = rospy.get_param(
            "~hook_command_topic",
            f"/{self.self_id}/hook/command",
        )
        self.takeoff_param_topic = rospy.get_param(
            "~takeoff_param_topic",
            f"/{self.self_id}/takeoff/param",
        )

        # ========================================================
        # 人工指令 topic
        # 这些默认是全局 topic，多台机都可以订阅。
        # 你后面可以改成自己的上位机/遥控输入节点。
        # ========================================================

        self.operator_takeoff_topic = rospy.get_param(
            "~operator_takeoff_topic",
            "/operator/takeoff",
        )

        self.operator_enter_follow_topic = rospy.get_param(
            "~operator_enter_follow_topic",
            "/operator/enter_follow",
        )
        self.operator_enter_follow_service = rospy.get_param(
            "~operator_enter_follow_service",
            f"/{self.self_id}/operator/enter_follow",
        )
        self.operator_enter_follow_service_wait_timeout = float(
            rospy.get_param("~operator_enter_follow_service_wait_timeout", 1.0)
        )

        self.operator_hook_sequence_topic = rospy.get_param(
            "~operator_hook_sequence_topic",
            "/operator/hook_sequence",
        )

        self.operator_land_topic = rospy.get_param(
            "~operator_land_topic",
            "/operator/land",
        )

        self.operator_emergency_hold_topic = rospy.get_param(
            "~operator_emergency_hold_topic",
            "/operator/emergency_hold",
        )

        # ========================================================
        # 频率参数
        # ========================================================

        self.control_rate_hz = float(rospy.get_param("~control_rate_hz", 50.0))
        self.status_rate = float(rospy.get_param("~task_status_rate", 20.0))
        self.mission_state_rate = float(rospy.get_param("~mission_state_rate", 10.0))
        self.request_rate = float(rospy.get_param("~task_request_rate", 1.0))
        self.sync_service_timeout = float(rospy.get_param("~sync_service_timeout", 0.2))
        self.auto_start = bool(rospy.get_param("~auto_start", False))

        # ========================================================
        # 任务参数
        # ========================================================

        self.takeoff_height = float(rospy.get_param("~takeoff_height", 3.0))
        self.takeoff_duration = float(rospy.get_param("~takeoff_duration", 6.0))
        self.takeoff_reached_tolerance = float(
            rospy.get_param("~takeoff_reached_tolerance", 0.25)
        )
        self.trajectory_reset_service = rospy.get_param(
            "~trajectory_reset_service",
            f"/{self.self_id}/trajectory/reset",
        )
        self.trajectory_reset_service_timeout = float(
            rospy.get_param("~trajectory_reset_service_timeout", 0.05)
        )
        self.trajectory_reset_retry_interval = float(
            rospy.get_param("~trajectory_reset_retry_interval", 1.0)
        )

        self.hook_to_retract_delay = float(
            rospy.get_param("~hook_to_retract_delay", 5.0)
        )

        self.land_timeout = float(rospy.get_param("~land_timeout", 30.0))
        self.use_offboard_landing = get_bool_param("~use_offboard_landing", True)
        self.transfer_to_landing_service = rospy.get_param(
            "~transfer_to_landing_service",
            f"/{self.self_id}/transfer_to_landing",
        )
        self.transfer_to_landing_service_timeout = float(
            rospy.get_param("~transfer_to_landing_service_timeout", 1.0)
        )

        # demo 模式：
        # 如果没有接真实高度、落地检测，可以先用时间自动通过部分判断。
        self.demo_mode = bool(rospy.get_param("~demo_mode", False))
        self.max_send_count = int(rospy.get_param("~max_send_count", 10))
        self.send_count = 0
        # ========================================================
        # 自检相关参数
        # ========================================================

        # 注意：
        # demo_mode=True 时，默认绕过真实自检，方便你跑自动流程测试。
        # 实飞或者半实物联调时建议：
        #   demo_mode:=false
        #   self_check_bypass:=false
        self.self_check_bypass = bool(
            rospy.get_param("~self_check_bypass", self.demo_mode)
        )

        self.require_mavros_connected = bool(
            rospy.get_param("~require_mavros_connected", True)
        )
        self.require_gps = bool(rospy.get_param("~require_gps", True))
        self.require_ekf = bool(rospy.get_param("~require_ekf", True))
        self.require_rc = bool(rospy.get_param("~require_rc", True))

        # 各类消息超时时间
        self.mavros_state_timeout = float(
            rospy.get_param("~mavros_state_timeout", 1.0)
        )
        self.gps_timeout = float(rospy.get_param("~gps_timeout", 1.0))
        self.ekf_timeout = float(rospy.get_param("~ekf_timeout", 1.0))
        self.rc_timeout = float(rospy.get_param("~rc_timeout", 1.0))

        # GPS 最低 fix 状态：
        # NavSatStatus.STATUS_NO_FIX = -1
        # NavSatStatus.STATUS_FIX    = 0
        # NavSatStatus.STATUS_SBAS_FIX = 1
        # NavSatStatus.STATUS_GBAS_FIX = 2
        #
        # 一般要求普通 GPS fix 用 0；
        # 如果你希望更严格，比如 RTK/GBAS，可以设成 2。
        self.gps_min_status = int(rospy.get_param("~gps_min_status", 0))

        # 遥控器检查
        self.rc_min_channels = int(rospy.get_param("~rc_min_channels", 4))

        # rc_min_rssi < 0 表示不检查 rssi，只检查 RCIn 是否新鲜、通道是否存在。
        # 因为有些接收机/链路 rssi 可能一直是 0 或未定义。
        self.rc_min_rssi = int(rospy.get_param("~rc_min_rssi", -1))

        # EKF2 状态检查项
        self.require_ekf_attitude = bool(
            rospy.get_param("~require_ekf_attitude", True)
        )
        self.require_ekf_vel_horiz = bool(
            rospy.get_param("~require_ekf_vel_horiz", True)
        )
        self.require_ekf_vel_vert = bool(
            rospy.get_param("~require_ekf_vel_vert", True)
        )
        self.require_ekf_pos_horiz_abs = bool(
            rospy.get_param("~require_ekf_pos_horiz_abs", True)
        )
        self.require_ekf_pos_vert_abs = bool(
            rospy.get_param("~require_ekf_pos_vert_abs", False)
        )

        self.reject_ekf_gps_glitch = bool(
            rospy.get_param("~reject_ekf_gps_glitch", True)
        )
        self.reject_ekf_accel_error = bool(
            rospy.get_param("~reject_ekf_accel_error", True)
        )

        # 自检日志打印限频
        self.self_check_log_period = float(
            rospy.get_param("~self_check_log_period", 1.0)
        )
        self.last_self_check_log_time = 0.0

        # MAVROS topic，可以根据你的命名空间调整。
        # 如果每台飞机的 mavros 都在 /mavros 下，就保持默认。
        # 如果是 /uav4/mavros，需要在 launch 里改。
        self.mavros_state_topic = rospy.get_param(
            "~mavros_state_topic",
            f"/{self.self_id}/mavros/state",
        )

        self.gps_topic = rospy.get_param(
            "~gps_topic",
            f"/{self.self_id}/mavros/global_position/global",
        )

        self.extended_state_topic = rospy.get_param(
            "~extended_state_topic",
            f"/{self.self_id}/mavros/extended_state",
        )

        self.estimator_status_topic = rospy.get_param(
            "~estimator_status_topic",
            f"/{self.self_id}/mavros/estimator_status",
        )

        self.rc_in_topic = rospy.get_param(
            "~rc_in_topic",
            f"/{self.self_id}/mavros/rc/in",
        )

        self.manual_control_topic = rospy.get_param(
            "~manual_control_topic",
            f"/{self.self_id}/mavros/manual_control/control",
        )

        # ========================================================
        # OFFBOARD / ARM 管理参数
        # ========================================================

        self.enable_offboard_manager = bool(
            rospy.get_param("~enable_offboard_manager", True)
        )

        self.auto_offboard = bool(
            rospy.get_param("~auto_offboard", True)
        )

        self.auto_arm = bool(
            rospy.get_param("~auto_arm", True)
        )

        # OFFBOARD / ARM 管理频率，建议 1~2Hz，不要 50Hz 调服务
        self.offboard_manage_rate = float(
            rospy.get_param("~offboard_manage_rate", 2.0)
        )
        self.offboard_arm_request_delay = float(
            rospy.get_param("~offboard_arm_request_delay", 0.0)
        )

        self.set_mode_min_interval = float(
            rospy.get_param("~set_mode_min_interval", 1.0)
        )

        self.arm_min_interval = float(
            rospy.get_param("~arm_min_interval", 1.0)
        )
        self.master_pilot_hold_px4_mode = str(
            rospy.get_param("~master_pilot_hold_px4_mode", "AUTO.LOITER")
        ).strip()
        self.enable_master_pilot_hold_mode = bool(
            rospy.get_param("~enable_master_pilot_hold_mode", True)
        )
        self.emergency_hold_px4_mode = str(
            rospy.get_param("~emergency_hold_px4_mode", self.master_pilot_hold_px4_mode)
        ).strip()
        self.enable_emergency_hold_mode = bool(
            rospy.get_param("~enable_emergency_hold_mode", True)
        )
        self.land_px4_mode = str(
            rospy.get_param("~land_px4_mode", "AUTO.LAND")
        ).strip()
        self.enable_px4_land_mode = bool(
            rospy.get_param("~enable_px4_land_mode", True)
        )
        self.return_px4_modes = {
            self._normalize_px4_mode(mode)
            for mode in self._parse_csv_param(
                rospy.get_param("~return_px4_modes", "AUTO.RTL,RTL,RETURN")
            )
        }

        # 收到 takeoff SCHEDULED 后，距离 t0 多少秒开始尝试 OFFBOARD / ARM
        self.prearm_before_t0 = float(
            rospy.get_param("~prearm_before_t0", 3.0)
        )

        # TAKEOFF_READY 且收到 takeoff 指令后，是否提前允许 OFFBOARD
        self.offboard_in_takeoff_ready_after_cmd = bool(
            rospy.get_param("~offboard_in_takeoff_ready_after_cmd", True)
        )

        # 三段拨杆通道，注意：这是遥控器第几通道，不是数组下标。
        # 低位：idle，不请求任何模式；
        # 中位：pos，请求 POSCTL；
        # 高位：offboard，允许程序请求 OFFBOARD/ARM。
        self.offboard_switch_channel = int(
            rospy.get_param("~offboard_switch_channel", 7)
        )

        # 三段拨杆中位/高位阈值。通常三段为 1000 / 1500 / 2000。
        self.offboard_switch_pos_threshold = int(
            rospy.get_param("~offboard_switch_pos_threshold", 1300)
        )
        self.offboard_switch_high_threshold = int(
            rospy.get_param("~offboard_switch_high_threshold", 1800)
        )

        # OFFBOARD/ARM 许可来源：
        #   always         调试用，始终允许程序切 OFFBOARD/ARM。
        #   rc_in          使用 /mavros/rc/in 的通道阈值逻辑。
        #   manual_control 使用 /mavros/manual_control/control 的 buttons 位。
        self.offboard_permission_source = str(
            rospy.get_param("~offboard_permission_source", "always")
        ).lower().strip()

        # QGC joystick -> MAVLink MANUAL_CONTROL -> MAVROS ManualControl。
        # 仿真可用 manual_control_offboard_bit 单 bit 允许 OFFBOARD/ARM。
        # 兼容旧配置：若 manual_control_offboard_bit < 0，则继续使用
        # SB 三段开关 bit7/bit6 编码：10 / 01 / 00，只有 00 允许。
        self.manual_control_timeout = float(
            rospy.get_param("~manual_control_timeout", self.rc_timeout)
        )
        self.manual_control_offboard_bit = int(
            rospy.get_param("~manual_control_offboard_bit", -1)
        )
        self.manual_control_offboard_active_value = int(
            rospy.get_param("~manual_control_offboard_active_value", 1)
        )
        self.manual_control_sb_high_bit = int(
            rospy.get_param(
                "~manual_control_sb_high_bit",
                rospy.get_param("~manual_control_sd_high_bit", 7),
            )
        )
        self.manual_control_sb_low_bit = int(
            rospy.get_param(
                "~manual_control_sb_low_bit",
                rospy.get_param("~manual_control_sd_low_bit", 6),
            )
        )
        # 任务阶段开关，仅 offboard_permission_source=manual_control 时生效。
        # 这里沿用 MAVROS ManualControl.buttons 的 bit 编号。
        self.manual_control_sa_bit = int(
            rospy.get_param("~manual_control_sa_bit", 3)
        )
        self.manual_control_sa_active_value = int(
            rospy.get_param("~manual_control_sa_active_value", 1)
        )
        self.manual_control_sd_bit = int(
            rospy.get_param("~manual_control_sd_bit", 1)
        )
        self.manual_control_sd_land_value = int(
            rospy.get_param("~manual_control_sd_land_value", 1)
        )
        self.manual_control_takeoff_bit = int(
            rospy.get_param("~manual_control_takeoff_bit", -1)
        )
        self.manual_control_takeoff_active_value = int(
            rospy.get_param("~manual_control_takeoff_active_value", 1)
        )
        self.manual_control_hook_bit = int(
            rospy.get_param("~manual_control_hook_bit", 2)
        )
        self.manual_control_hook_value = int(
            rospy.get_param("~manual_control_hook_value", 1)
        )
        self.rc_task_land_channel = int(
            rospy.get_param("~rc_task_land_channel", 0)
        )
        self.rc_task_hook_channel = int(
            rospy.get_param("~rc_task_hook_channel", 0)
        )
        self.rc_task_takeoff_channel = int(
            rospy.get_param(
                "~rc_task_takeoff_channel",
                rospy.get_param("~rc_takeoff_hold_channel", 0),
            )
        )
        self.rc_takeoff_advance_channel = int(
            rospy.get_param("~rc_takeoff_advance_channel", 0)
        )
        self.rc_emergency_hold_channel = int(
            rospy.get_param("~rc_emergency_hold_channel", 0)
        )
        rc_task_switch_default_threshold = int(
            rospy.get_param("~rc_task_switch_high_threshold", 1800)
        )
        self.rc_task_land_threshold = int(
            rospy.get_param("~rc_task_land_threshold", rc_task_switch_default_threshold)
        )
        self.rc_task_land_direction = str(
            rospy.get_param("~rc_task_land_direction", "above")
        ).lower().strip()
        self.rc_task_hook_threshold = int(
            rospy.get_param("~rc_task_hook_threshold", rc_task_switch_default_threshold)
        )
        self.rc_task_hook_direction = str(
            rospy.get_param("~rc_task_hook_direction", "above")
        ).lower().strip()
        self.rc_task_takeoff_threshold = int(
            rospy.get_param(
                "~rc_task_takeoff_threshold",
                rospy.get_param(
                    "~rc_takeoff_hold_threshold",
                    rc_task_switch_default_threshold,
                ),
            )
        )
        self.rc_task_takeoff_direction = str(
            rospy.get_param(
                "~rc_task_takeoff_direction",
                rospy.get_param("~rc_takeoff_hold_direction", "above"),
            )
        ).lower().strip()
        self.rc_takeoff_advance_threshold = int(
            rospy.get_param(
                "~rc_takeoff_advance_threshold",
                rc_task_switch_default_threshold,
            )
        )
        self.rc_takeoff_advance_direction = str(
            rospy.get_param("~rc_takeoff_advance_direction", "above")
        ).lower().strip()
        self.rc_emergency_hold_threshold = int(
            rospy.get_param(
                "~rc_emergency_hold_threshold",
                rc_task_switch_default_threshold,
            )
        )
        self.rc_emergency_hold_direction = str(
            rospy.get_param("~rc_emergency_hold_direction", "above")
        ).lower().strip()

        # CH9 三段开关模式：unknown / idle / pos / offboard。
        self.rc_program_switch_mode = "unknown"

        # 当前三段拨杆是否允许程序干预，只有 offboard 档为 True。
        self.rc_offboard_permission = False

        # 内部计时，避免频繁调用服务
        self.last_offboard_manage_time = 0.0
        self.last_set_mode_req_time = 0.0
        self.last_arm_req_time = 0.0

        # MAVROS service，按 self_id 拼接
        self.set_mode_service = rospy.get_param(
            "~set_mode_service",
            f"/{self.self_id}/mavros/set_mode",
        )

        self.arming_service = rospy.get_param(
            "~arming_service",
            f"/{self.self_id}/mavros/cmd/arming",
        )

        self.set_mode_srv = rospy.ServiceProxy(
            self.set_mode_service,
            SetMode,
        )

        self.arming_srv = rospy.ServiceProxy(
            self.arming_service,
            CommandBool,
        )
        self.transfer_to_landing_srv = rospy.ServiceProxy(
            self.transfer_to_landing_service,
            JsonPayload,
        )

        # ========================================================
        # 自检状态缓存
        # ========================================================

        self.last_mavros_state_time = 0.0
        self.mavros_connected = False
        self.mavros_mode = ""
        self.mavros_armed = False

        self.last_gps_time = 0.0
        self.gps_fix_status = -1
        self.gps_lat = float("nan")
        self.gps_lon = float("nan")
        self.gps_alt = float("nan")
        self.last_extended_state_time = 0.0
        self.landed_state = int(getattr(ExtendedState, "LANDED_STATE_UNDEFINED", 0))

        self.last_ekf_time = 0.0
        self.ekf_flags = {
            "attitude": False,
            "vel_horiz": False,
            "vel_vert": False,
            "pos_horiz_abs": False,
            "pos_vert_abs": False,
            "gps_glitch": False,
            "accel_error": False,
        }

        self.last_rc_time = 0.0
        self.rc_channels = []
        self.rc_rssi = 0
        self.last_manual_control_time = 0.0
        self.manual_control_buttons = 0
        self.manual_control_x = 0.0
        self.manual_control_y = 0.0
        self.manual_control_z = 0.0
        self.manual_control_r = 0.0
        self.task_switch_takeoff_active_last = False
        self.task_switch_hook_active_last = False

        self.self_check_report: Dict[str, Any] = {}

        # ========================================================
        # 人工指令标志位
        # ========================================================

        self.cmd_takeoff = False
        self.cmd_enter_follow = False
        self.cmd_hook_sequence = False
        self.cmd_land = False
        self.cmd_emergency_hold = False

        # ========================================================
        # 任务状态初始化
        # ========================================================

        if self.role == "master":
            self.task_state = self.TASK_IDLE
        else:
            # slave 不需要人工 start，可以先自检，然后等待 master 发起同步起飞。
            self.task_state = self.TASK_SELF_CHECK

        self.prev_task_state = ""
        self.state_enter_time = now_sec()

        # 当前同步动作信息
        self.current_action = ""
        self.current_payload: Dict[str, Any] = {}
        self.current_sync_id = ""
        self.current_t0 = 0.0
        self.action_start_time = 0.0

        # 已经启动过的同步会话 ID。
        #
        # 用途：防止同一个 sync_id 被启动两次。
        # 原因：TaskFSM 有两条启动路径：
        #   1. 收到 SCHEDULED 后，主循环在 now >= t0 时主动启动；
        #   2. 收到 SyncGate 的 START 兜底事件后启动。
        # 这两条路径可能在 t0 附近几乎同时触发，因此必须按 sync_id 去重。
        self.started_sync_ids = set()

        # SCHEDULED 事件缓存。
        self.scheduled_event: Optional[dict] = None

        # master 防止重复发送 sync_request。
        self.request_in_flight = False
        self.last_request_pub_time = 0.0
        self.request_counter = 0

        # 松钩/收绳内部阶段
        self.hook_sequence_start_time = 0.0
        self.hook_released = False
        self.rope_retracted = False
        self.return_state_after_hook = ""

        # 降落开始时间
        self.land_start_time = 0.0
        self.master_pilot_hold_mode_reached = False
        self.emergency_hold_mode_reached = False
        self.trajectory_reset_success_in_self_check = False
        self.last_trajectory_reset_req_time = 0.0

        # 状态发布限频
        self.last_status_pub_time = 0.0
        self.last_mission_state_pub_time = 0.0

        # ========================================================
        # 状态处理函数映射
        # ========================================================

        self.state_handlers: Dict[str, Callable[[], None]] = {
            self.TASK_IDLE: self._state_idle,
            self.TASK_SELF_CHECK: self._state_self_check,
            self.TASK_TAKEOFF_READY: self._state_takeoff_ready,
            self.TASK_TAKEOFF_RUNNING: self._state_takeoff_running,
            self.TASK_WAIT_ENTER_FOLLOW_CMD: self._state_wait_enter_follow_cmd,
            self.TASK_MASTER_PILOT_HOLD: self._state_master_pilot_hold,
            self.TASK_FOLLOW_MASTER: self._state_follow_master,
            self.TASK_HOOK_SEQUENCE_RUNNING: self._state_hook_sequence_running,
            self.TASK_LAND_RUNNING: self._state_land_running,
            self.TASK_RESETTING: self._state_resetting,
            self.TASK_EMERGENCY_HOLD: self._state_emergency_hold,
            self.TASK_ABORTED: self._state_aborted,
        }

        # ========================================================
        # ROS pub/sub/service
        # ========================================================

        self.status_srv = rospy.ServiceProxy(self.task_status_topic, JsonPayload)
        self.request_srv = rospy.ServiceProxy(self.task_request_topic, JsonPayload)

        self.mission_state_pub = rospy.Publisher(
            self.mission_state_topic,
            String,
            queue_size=50,
        )

        self.planner_request_pub = rospy.Publisher(
            self.planner_request_topic,
            String,
            queue_size=20,
        )

        self.controller_command_pub = rospy.Publisher(
            self.controller_command_topic,
            String,
            queue_size=50,
        )

        self.hook_command_pub = rospy.Publisher(
            self.hook_command_topic,
            String,
            queue_size=20,
        )
        # 发布起飞相关参数给轨迹规划器
        self.takeoff_param_pub = rospy.Publisher(
            self.takeoff_param_topic,
            String,
            queue_size=20,
        )

        self.task_event_srv = rospy.Service(
            self.task_event_topic,
            JsonPayload,
            self._sync_event_srv,
        )

        rospy.Subscriber(
            self.start_topic,
            Bool,
            self._start_cb,
            queue_size=5,
        )

        rospy.Subscriber(
            self.operator_takeoff_topic,
            Bool,
            self._operator_takeoff_cb,
            queue_size=5,
        )

        rospy.Subscriber(
            self.operator_enter_follow_topic,
            Bool,
            self._operator_enter_follow_cb,
            queue_size=5,
        )

        self.operator_enter_follow_srv = rospy.Service(
            self.operator_enter_follow_service,
            Trigger,
            self._operator_enter_follow_srv,
        )

        rospy.Subscriber(
            self.operator_hook_sequence_topic,
            Bool,
            self._operator_hook_sequence_cb,
            queue_size=5,
        )

        rospy.Subscriber(
            self.operator_land_topic,
            Bool,
            self._operator_land_cb,
            queue_size=5,
        )

        rospy.Subscriber(
            self.operator_emergency_hold_topic,
            Bool,
            self._operator_emergency_hold_cb,
            queue_size=5,
        )

        rospy.Subscriber(
            self.mavros_state_topic,
            State,
            self._mavros_state_cb,
            queue_size=20,
        )

        rospy.Subscriber(
            self.gps_topic,
            NavSatFix,
            self._gps_cb,
            queue_size=20,
        )

        rospy.Subscriber(
            self.extended_state_topic,
            ExtendedState,
            self._extended_state_cb,
            queue_size=20,
        )

        if EstimatorStatus is not None:
            rospy.Subscriber(
                self.estimator_status_topic,
                EstimatorStatus,
                self._estimator_status_cb,
                queue_size=20,
            )
        elif self.require_ekf:
            rospy.logwarn(
                "[%s TaskFSM] mavros_msgs/EstimatorStatus not available; "
                "EKF self-check will not pass unless ~require_ekf is false",
                self.self_id,
            )

        rospy.Subscriber(
            self.rc_in_topic,
            RCIn,
            self._rc_in_cb,
            queue_size=20,
        )

        rospy.Subscriber(
            self.manual_control_topic,
            ManualControl,
            self._manual_control_cb,
            queue_size=20,
        )

        rospy.loginfo(
            "[TaskFSMNode] role=%s self_id=%s init_state=%s offboard_permission_source=%s",
            self.role,
            self.self_id,
            self.task_state,
            self.offboard_permission_source,
        )

    @staticmethod
    def _parse_csv_param(value: Any) -> list:
        if isinstance(value, (list, tuple)):
            return [str(item).strip() for item in value if str(item).strip()]

        return [item.strip() for item in str(value).split(",") if item.strip()]

    @staticmethod
    def _normalize_px4_mode(mode: str) -> str:
        return str(mode or "").strip().upper()

    # ============================================================
    # 自检输入回调
    # ============================================================

    def _mavros_state_cb(self, msg: State):
        """
        /mavros/state：
            - connected 用于 MAVROS 连接检查；
            - mode/armed 进入自检报告，方便排查。
        """
        self.last_mavros_state_time = now_sec()
        self.mavros_connected = bool(msg.connected)
        self.mavros_mode = msg.mode
        self.mavros_armed = bool(msg.armed)

    def _gps_cb(self, msg: NavSatFix):
        """
        /mavros/global_position/global：
            - status.status 用于 GPS fix 检查；
            - latitude/longitude/altitude 用于有效数值检查。
        """
        self.last_gps_time = now_sec()
        self.gps_fix_status = int(msg.status.status)
        self.gps_lat = float(msg.latitude)
        self.gps_lon = float(msg.longitude)
        self.gps_alt = float(msg.altitude)

    def _extended_state_cb(self, msg: ExtendedState):
        self.last_extended_state_time = now_sec()
        self.landed_state = int(msg.landed_state)

    def _estimator_status_cb(self, msg):
        """
        /mavros/estimator_status：
            将 MAVROS EKF2 estimator flags 缓存成自检使用的布尔量。
        """
        self.last_ekf_time = now_sec()
        self.ekf_flags["attitude"] = bool(msg.attitude_status_flag)
        self.ekf_flags["vel_horiz"] = bool(msg.velocity_horiz_status_flag)
        self.ekf_flags["vel_vert"] = bool(msg.velocity_vert_status_flag)
        self.ekf_flags["pos_horiz_abs"] = bool(msg.pos_horiz_abs_status_flag)
        self.ekf_flags["pos_vert_abs"] = bool(msg.pos_vert_abs_status_flag)
        self.ekf_flags["gps_glitch"] = bool(msg.gps_glitch_status_flag)
        self.ekf_flags["accel_error"] = bool(msg.accel_error_status_flag)

    def _rc_in_cb(self, msg: RCIn):
        """
        /mavros/rc/in：
            - channels 用于通道数检查；
            - rssi 在 rc_min_rssi >= 0 时参与检查。
        """
        now = now_sec()
        self.last_rc_time = now
        self.rc_channels = list(msg.channels)
        self.rc_rssi = int(msg.rssi)
        self._update_rc_offboard_permission(now)

    def _manual_control_cb(self, msg: ManualControl):
        """
        /mavros/manual_control/control：
            QGC joystick / 虚拟摇杆通常通过 MAVLink MANUAL_CONTROL 到飞控，
            MAVROS 在这里给出归一化杆量和 buttons 位图。
        """
        now = now_sec()
        self.last_manual_control_time = now
        self.manual_control_x = float(msg.x)
        self.manual_control_y = float(msg.y)
        self.manual_control_z = float(msg.z)
        self.manual_control_r = float(msg.r)
        self.manual_control_buttons = int(msg.buttons)
        self._update_rc_offboard_permission(now)

    def _update_rc_offboard_permission(self, t: Optional[float] = None):
        """
        根据配置的许可来源刷新 OFFBOARD/ARM 许可。
        """
        if t is None:
            t = now_sec()

        if self._is_always_permission_strategy():
            self._set_offboard_permission(True, "always")
            return

        if self._is_manual_control_strategy():
            self._update_manual_control_offboard_permission(t)
            return

        if self._is_rc_in_strategy():
            self._update_rc_in_offboard_permission(t)
            return

        rospy.logwarn_throttle(
            2.0,
            "[%s TaskFSM] unknown offboard_permission_source=%s; program control disabled",
            self.self_id,
            self.offboard_permission_source,
        )
        self._set_offboard_permission(False, "unknown_source")

    def _update_rc_in_offboard_permission(self, t: float):
        """
        根据 RCIn 中的三段拨杆通道刷新程序控制模式。

        offboard_switch_channel 是遥控器通道号，从 1 开始；
        RCIn.channels 是 Python 数组，从 0 开始。
        """
        new_mode = "unknown"
        new_permission = False

        channel_index = self.offboard_switch_channel - 1

        if self.offboard_switch_channel <= 0:
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] invalid offboard_switch_channel=%s; program control disabled",
                self.self_id,
                self.offboard_switch_channel,
            )
        elif self.last_rc_time <= 0.0 or (t - self.last_rc_time) > self.rc_timeout:
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] RCIn timeout; program OFFBOARD/ARM permission disabled",
                self.self_id,
            )
        elif len(self.rc_channels) <= channel_index:
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] RCIn has %d channels, need channel %d for OFFBOARD permission",
                self.self_id,
                len(self.rc_channels),
                self.offboard_switch_channel,
            )
        else:
            switch_value = int(self.rc_channels[channel_index])
            new_mode = self._rc_program_switch_mode_from_value(switch_value)
            new_permission = new_mode == "offboard"

        self._set_rc_program_switch_mode(new_mode)
        self._set_offboard_permission(new_permission, "rc_in")

    def _rc_program_switch_mode_from_value(self, value: int) -> str:
        if value >= self.offboard_switch_high_threshold:
            return "offboard"
        if value >= self.offboard_switch_pos_threshold:
            return "pos"
        return "idle"

    def _set_rc_program_switch_mode(self, new_mode: str):
        old_mode = self.rc_program_switch_mode
        if old_mode == new_mode:
            return

        self.rc_program_switch_mode = new_mode
        rospy.logwarn(
            "[%s TaskFSM] CH%s program switch mode changed: %s -> %s value=%s thresholds(pos=%s,offboard=%s)",
            self.self_id,
            self.offboard_switch_channel,
            old_mode,
            new_mode,
            self._rc_channel_value(self.offboard_switch_channel),
            self.offboard_switch_pos_threshold,
            self.offboard_switch_high_threshold,
        )

    def _update_manual_control_offboard_permission(self, t: float):
        """
        根据 ManualControl.buttons 刷新 OFFBOARD/ARM 许可。

        manual_control_offboard_bit >= 0 时使用单 bit 电平。

        旧 SB fallback 默认由 bit7/bit6 编码：
            10: 不允许
            01: 不允许
            00: 允许 OFFBOARD/ARM
        """
        if self.last_manual_control_time <= 0.0 or (
            t - self.last_manual_control_time
        ) > self.manual_control_timeout:
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] ManualControl timeout; program OFFBOARD/ARM permission disabled",
                self.self_id,
            )
            self._set_offboard_permission(False, "manual_control_timeout")
            return

        if self.manual_control_offboard_bit >= 0:
            bit_value = self._manual_control_bit(self.manual_control_offboard_bit)
            new_permission = (
                bit_value == self.manual_control_offboard_active_value
            )
            self._set_offboard_permission(
                new_permission,
                "manual_control_offboard_bit{}={}".format(
                    self.manual_control_offboard_bit,
                    bit_value,
                ),
            )
            return

        if self.manual_control_sb_high_bit < 0 or self.manual_control_sb_low_bit < 0:
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] invalid manual_control SB bits high=%s low=%s; program control disabled",
                self.self_id,
                self.manual_control_sb_high_bit,
                self.manual_control_sb_low_bit,
            )
            self._set_offboard_permission(False, "invalid_manual_control_bits")
            return

        high = (self.manual_control_buttons >> self.manual_control_sb_high_bit) & 0x01
        low = (self.manual_control_buttons >> self.manual_control_sb_low_bit) & 0x01
        new_permission = (high == 0) and (low == 0)

        self._set_offboard_permission(new_permission, f"manual_control_sb={high}{low}")

    def _is_always_permission_strategy(self) -> bool:
        return self.offboard_permission_source in ["always", "true", "demo"]

    def _is_manual_control_strategy(self) -> bool:
        return self.offboard_permission_source in [
            "manual",
            "manual_control",
            "qgc_joystick",
        ]

    def _is_rc_in_strategy(self) -> bool:
        return self.offboard_permission_source in ["rc", "rc_in"]

    def _manual_control_fresh(self, t: Optional[float] = None) -> bool:
        if t is None:
            t = now_sec()
        return (
            self.last_manual_control_time > 0.0
            and (t - self.last_manual_control_time) <= self.manual_control_timeout
        )

    def _manual_control_bit(self, bit_index: int) -> int:
        rospy.logdebug_throttle(
            5.0,
            "[%s TaskFSM] manual_control_buttons=0b%s, checking bit_index=%d",
            self.self_id,
            format(self.manual_control_buttons, '08b'),
            bit_index,
        )
        if bit_index < 0:
            return 0

        return (self.manual_control_buttons >> bit_index) & 0x01

    def _task_switch_takeoff_advance_allowed(self) -> bool:
        """
        起飞运行阶段是否允许进入下一任务阶段。

        slave：
            不检测本机 rc_takeoff_advance_channel。
            master 在自己的 advance 通道有效后，会通过
            /<slave_id>/operator/enter_follow service 下发放行命令。

        manual_control 策略：
            manual_control_sa_bit=1 才允许
            TASK_TAKEOFF_RUNNING -> TASK_WAIT_ENTER_FOLLOW_CMD。

        rc_in 策略：
            只有 master 读取本机 RCIn。
            由 rc_takeoff_advance_channel 控制是否允许进入下一阶段。
        """
        if self.role == "slave":
            return self.cmd_enter_follow

        if self._is_manual_control_strategy():
            if not self._manual_control_fresh():
                rospy.logwarn_throttle(
                    2.0,
                    "[%s TaskFSM] wait manual_control advance bit, but ManualControl is not fresh",
                    self.self_id,
                )
                return False

            sa = self._manual_control_bit(self.manual_control_sa_bit)
            allowed = sa == self.manual_control_sa_active_value
            if not allowed:
                rospy.loginfo_throttle(
                    2.0,
                    "[%s TaskFSM] takeoff reached; waiting manual_control advance bit=%s, current=%s",
                    self.self_id,
                    self.manual_control_sa_active_value,
                    sa,
                )
            return allowed

        if self._is_rc_in_strategy():
            return self._rc_task_switch_takeoff_advance_allowed()
        # TODO 实现RC策略

        return True

    def _task_switch_takeoff_requested(self) -> bool:
        """
        起飞同步触发由当前输入策略的上升沿触发。

        manual_control 策略：
            manual_control_takeoff_bit 由 QGC joystick buttons 位触发。

        rc_in 策略：
            rc_task_takeoff_channel 由 MAVROS RCIn 通道触发。
        """
        active = False

        if self._is_manual_control_strategy():
            active = (
                self.manual_control_takeoff_bit >= 0
                and self._manual_control_fresh()
                and self._manual_control_bit(self.manual_control_takeoff_bit)
                == self.manual_control_takeoff_active_value
            )
        elif self._is_rc_in_strategy():
            active = self._rc_task_switch_takeoff_requested()

        requested = active and not self.task_switch_takeoff_active_last
        self.task_switch_takeoff_active_last = active
        return requested

    def _task_switch_land_requested(self) -> bool:
        """
        是否由任务开关请求降落。

        manual_control 策略：
            SD=1 请求结束任务并进入 land 同步。

        rc_in 策略：
            先留占位，当前不触发降落。
        """
        if self._is_manual_control_strategy():
            if not self._manual_control_fresh():
                return False
            return self._manual_control_bit(self.manual_control_sd_bit) == self.manual_control_sd_land_value

        if self._is_rc_in_strategy():
            return self._rc_task_switch_land_requested()

        return False

    def _task_switch_hook_requested(self) -> bool:
        """
        hook 释放任务开关，上升沿触发，避免按钮保持时重复进入 hook。
        """
        active = False
        if self._is_manual_control_strategy():
            active = (
                self._manual_control_fresh()
                and self._manual_control_bit(self.manual_control_hook_bit)
                == self.manual_control_hook_value
            )
        if self._is_rc_in_strategy():
            active = self._rc_task_switch_hook_requested()

        requested = active and not self.task_switch_hook_active_last
        self.task_switch_hook_active_last = active
        return requested

    def _task_switch_emergency_hold_active(self) -> bool:
        """
        紧急 HOLD 通道是电平触发并锁存的安全输入。

        只要任意一帧 RCIn 满足阈值，就会置 cmd_emergency_hold=True；
        后续不依赖该通道保持 active。
        """
        return self._rc_task_switch_emergency_hold_active()

    def _rc_task_switch_takeoff_advance_allowed(self) -> bool:
        if self.rc_takeoff_advance_channel <= 0:
            return True

        return self._rc_channel_matches(
            self.rc_takeoff_advance_channel,
            self.rc_takeoff_advance_threshold,
            self.rc_takeoff_advance_direction,
        )

    def _rc_task_switch_takeoff_requested(self) -> bool:
        return self._rc_channel_matches(
            self.rc_task_takeoff_channel,
            self.rc_task_takeoff_threshold,
            self.rc_task_takeoff_direction,
        )

    def _rc_task_switch_land_requested(self) -> bool:
        return self._rc_channel_matches(
            self.rc_task_land_channel,
            self.rc_task_land_threshold,
            self.rc_task_land_direction,
        )

    def _rc_task_switch_hook_requested(self) -> bool:
        return self._rc_channel_matches(
            self.rc_task_hook_channel,
            self.rc_task_hook_threshold,
            self.rc_task_hook_direction,
        )

    def _rc_task_switch_emergency_hold_active(self) -> bool:
        return self._rc_channel_matches(
            self.rc_emergency_hold_channel,
            self.rc_emergency_hold_threshold,
            self.rc_emergency_hold_direction,
        )

    def _rc_channel_matches(self, channel: int, threshold: int, direction: str) -> bool:
        if channel <= 0:
            return False

        t = now_sec()
        if self.last_rc_time <= 0.0 or (t - self.last_rc_time) > self.rc_timeout:
            return False

        channel_index = channel - 1
        if len(self.rc_channels) <= channel_index:
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] RCIn has %d channels, need channel %d for task switch",
                self.self_id,
                len(self.rc_channels),
                channel,
            )
            return False

        value = int(self.rc_channels[channel_index])
        if direction in ["above", "high", "greater", ">=", "gt"]:
            return value >= threshold
        if direction in ["below", "low", "less", "<=", "lt"]:
            return value <= threshold

        rospy.logwarn_throttle(
            2.0,
            "[%s TaskFSM] invalid RC task switch direction=%s for channel %d",
            self.self_id,
            direction,
            channel,
        )
        return False

    def _rc_channel_value(self, channel: int):
        if channel <= 0:
            return None

        channel_index = channel - 1
        if len(self.rc_channels) <= channel_index:
            return None

        return int(self.rc_channels[channel_index])

    def _handle_follow_stage_actions(self, return_state: str) -> bool:
        """
        主从/飞手接管阶段的任务入口。

        这些动作放在这里，是因为 hook 和 land 只有在起飞完成、进入
        MASTER_PILOT_HOLD / FOLLOW_MASTER 后才有任务意义。状态函数只负责
        维持当前飞行控制形态，然后把阶段内任务入口委托给本函数。
        """
        if (
            self.role == "master"
            and return_state == self.TASK_MASTER_PILOT_HOLD
            and (self.cmd_land or self._task_switch_land_requested())
        ):
            self._master_request_sync_if_needed(
                action="land",
                payload={
                    "mode": (
                        "TRAJECTORY_LANDING"
                        if self.use_offboard_landing
                        else "PX4_LAND"
                    ),
                    "requested_by": "operator",
                },
            )
            return True

        if self.cmd_hook_sequence or self._task_switch_hook_requested():
            self._begin_hook_sequence(return_state=return_state)
            return True

        return False

    def _set_offboard_permission(self, new_permission: bool, reason: str = ""):
        old_permission = self.rc_offboard_permission
        if old_permission == new_permission:
            return

        self.rc_offboard_permission = new_permission
        self._handle_offboard_permission_changed(
            old_permission,
            new_permission,
            reason,
        )

    def _handle_offboard_permission_changed(
        self,
        old_permission: bool,
        new_permission: bool,
        reason: str = "",
    ):
        """
        三段拨杆许可状态变化处理。

        old=True, new=False:
            飞手从程序干预档拨回低/中档；
            程序停止抢 OFFBOARD；
            只有拨到 CH9 中位 pos 时才请求 POSCTL；
            同时通知控制器进入人工接管 / passive。

        old=False, new=True:
            飞手拨到高位；
            这里只表示允许程序干预；
            不代表立刻切 OFFBOARD。
        """
        rospy.logwarn(
            "[%s TaskFSM] offboard permission changed: %s -> %s source=%s reason=%s",
            self.self_id,
            old_permission,
            new_permission,
            self.offboard_permission_source,
            reason,
        )

        if old_permission and not new_permission:
            if self.cmd_emergency_hold:
                rospy.logwarn(
                    "[%s TaskFSM] emergency hold is latched; skip POSCTL request on permission revoke",
                    self.self_id,
                )
                return

            if self.rc_program_switch_mode != "pos":
                rospy.logwarn(
                    "[%s TaskFSM] RC switch leaves OFFBOARD to %s; no mode request",
                    self.self_id,
                    self.rc_program_switch_mode,
                )
                return

            rospy.logwarn(
                "[%s TaskFSM] RC switch enters POS position, request POSCTL",
                self.self_id,
            )

            self._request_position_mode()

            # self.publish_controller_command(
            #     mode="PILOT_TAKEOVER",
            #     payload={
            #         "reason": "offboard_permission_revoked",
            #         "target_px4_mode": "POSCTL",
            #     },
            # )

        elif (not old_permission) and new_permission:
            rospy.logwarn(
                "[%s TaskFSM] switch allows program OFFBOARD control",
                self.self_id,
            )

    # ============================================================
    # 人工指令回调
    # ============================================================

    def _start_cb(self, msg: Bool):
        """
        master 外部启动任务流程。
        例如：
            rostopic pub /master/task/start std_msgs/Bool "data: true" -1
        """
        if msg.data:
            self.start()

    def start(self):
        if self.role != "master":
            return

        if self.task_state == self.TASK_IDLE:
            self._set_state(self.TASK_SELF_CHECK)

    def _operator_takeoff_cb(self, msg: Bool):
        """
        兼容保留的人工起飞 topic。

        实飞遥控器模式下，起飞只允许由 RCIn 的 rc_task_takeoff_channel 触发。
        """
        if msg.data:
            rospy.logwarn(
                "[%s TaskFSM] ignore /operator/takeoff; takeoff is RC channel %s only",
                self.self_id,
                self.rc_task_takeoff_channel,
            )

    def _operator_enter_follow_cb(self, msg: Bool):
        """
        起飞到指定高度后，人工确认进入主从飞行阶段。

        不走同步器。
        master：进入 MASTER_PILOT_HOLD。
        slave ：进入 FOLLOW_MASTER。
        """
        if msg.data:
            self.cmd_enter_follow = True

    def _operator_enter_follow_srv(self, _req):
        self.cmd_enter_follow = True
        rospy.loginfo("[%s TaskFSM] enter_follow service accepted", self.self_id)
        return TriggerResponse(success=True, message="enter_follow accepted")

    def _operator_hook_sequence_cb(self, msg: Bool):
        """
        人工触发松钩/收绳流程。

        不走同步器。
        飞行控制权保持原状态：
            master 仍然飞手/hold；
            slave 仍然跟随 master。
        """
        if msg.data:
            self.cmd_hook_sequence = True

    def _operator_land_cb(self, msg: Bool):
        """
        人工触发同步降落。

        master 在主从飞行阶段收到后，通过同步器发起 action="land"。
        slave 不主动 request，只等待同步器事件。
        """
        if msg.data:
            self.cmd_land = True

    def _operator_emergency_hold_cb(self, msg: Bool):
        """
        紧急保持。

        不走同步器。
        每架飞机收到后立即进入软件 HOLD。
        """
        if msg.data:
            self.cmd_emergency_hold = True

    # ============================================================
    # sync_gate_node -> task_fsm_node 事件回调
    # ============================================================

    def _sync_event_srv(self, req):
        data = safe_json_loads(req.payload)
        if not data:
            return JsonPayloadResponse(
                ok=False,
                reason="invalid_json",
                payload="",
            )

        self._handle_sync_event(data)
        return JsonPayloadResponse(
            ok=True,
            reason="ok",
            payload="",
        )

    def _handle_sync_event(self, data: dict):
        """
        处理同步器发来的事件。

        这里仍然不处理 PREPARE / COMMIT / ACK。
        这里只处理：
            REQUEST_ACCEPTED
            REQUEST_REJECTED
            SCHEDULED
            START
            ABORT
        """
        event = data.get("event", "")
        action = data.get("action", "")

        if event == EVENT_REQUEST_ACCEPTED:
            self.request_in_flight = True
            rospy.loginfo(
                "[%s TaskFSM] sync request accepted action=%s sync_id=%s",
                self.self_id,
                action,
                data.get("sync_id", ""),
            )

        elif event == EVENT_REQUEST_REJECTED:
            self.request_in_flight = False
            rospy.logwarn(
                "[%s TaskFSM] sync request rejected action=%s reason=%s",
                self.self_id,
                action,
                data.get("reason", ""),
            )

        elif event == EVENT_SCHEDULED:
            expected = self.expected_action()

            if action == expected:
                self.scheduled_event = data
                rospy.loginfo(
                    "[%s TaskFSM] scheduled action=%s t0=%.3f sync_id=%s",
                    self.self_id,
                    action,
                    float(data.get("t0", 0.0)),
                    data.get("sync_id", ""),
                )
            else:
                rospy.logwarn(
                    "[%s TaskFSM] ignore scheduled action=%s expected=%s state=%s",
                    self.self_id,
                    action,
                    expected,
                    self.task_state,
                )

        elif event == EVENT_START:
            # START 是兜底。
            # 正常情况下，FSM 会在 SCHEDULED 后自己判断 now >= t0 启动。
            expected = self.expected_action()

            if action == expected:
                self._start_action_from_event(data)

        elif event == EVENT_ABORT:
            rospy.logerr(
                "[%s TaskFSM] sync abort action=%s reason=%s",
                self.self_id,
                action,
                data.get("reason", ""),
            )

            self.request_in_flight = False
            self.scheduled_event = None
            self._set_state(self.TASK_ABORTED)

    # ============================================================
    # 主循环
    # ============================================================

    def spin(self):
        rate = rospy.Rate(self.control_rate_hz)

        while not rospy.is_shutdown():
            if self.role == "master" and self.auto_start:
                self.start()

            # 紧急保持优先级最高。
            self._check_emergency_first()

            # 根据配置来源刷新 OFFBOARD / ARM 许可。
            self._update_rc_offboard_permission()

            # 向 sync_gate_node 发布同步状态。
            self._publish_sync_status_periodically()

            # 向其他模块发布任务状态，方便轨迹规划器、控制器、监控节点使用。
            self._publish_mission_state_periodically()

            # 收到 SCHEDULED 后，任务层自己按 t0 启动。
            self._start_scheduled_action_if_due()

            # 低频管理 OFFBOARD / ARM
            self._manage_offboard_and_arm_low_rate()

            # 当前状态处理。
            self._tick_task_50hz()

            rate.sleep()

    def _tick_task_50hz(self):
        handler = self.state_handlers.get(self.task_state, self._state_unknown)
        handler()

    def _check_emergency_first(self):
        """
        紧急保持优先级最高。

        只要收到 emergency_hold 指令，就切入软件 HOLD。
        不交还 PX4 HOLD，由外部控制器持续发速度/位置保持指令。
        """
        if self._task_switch_emergency_hold_active():
            if not self.cmd_emergency_hold:
                rospy.logerr(
                    "[%s TaskFSM] RC emergency hold triggered on channel %s",
                    self.self_id,
                    self.rc_emergency_hold_channel,
                )
            self.cmd_emergency_hold = True

        if not self.cmd_emergency_hold:
            return

        if self.task_state != self.TASK_EMERGENCY_HOLD:
            self.request_in_flight = False
            self.scheduled_event = None
            self._set_state(self.TASK_EMERGENCY_HOLD)



    def _state_requires_offboard(self) -> bool:
        """
        判断当前任务状态是否需要 PX4 处于 OFFBOARD。

        注意：
            这里只判断任务状态是否需要 OFFBOARD；
            最终是否真的请求 OFFBOARD，还要看 rc_offboard_permission。
        """
        if not self.enable_offboard_manager:
            return False

        if self._offboard_landing_active():
            return True

        if self.task_state in [
            self.TASK_TAKEOFF_RUNNING,
            self.TASK_WAIT_ENTER_FOLLOW_CMD,
            self.TASK_FOLLOW_MASTER,
            self.TASK_HOOK_SEQUENCE_RUNNING,
        ]:
            return True

        # TAKEOFF_READY 下，如果 master 收到起飞指令，可以提前准备 OFFBOARD
        if (
            self.task_state == self.TASK_TAKEOFF_READY
            and self.offboard_in_takeoff_ready_after_cmd
            and self.cmd_takeoff
        ):
            return True

        # 收到 takeoff 的 SCHEDULED 后，在 t0 前 prearm_before_t0 秒内提前准备
        if self.scheduled_event is not None:
            action = self.scheduled_event.get("action", "")
            if action == "takeoff":
                try:
                    t0 = float(self.scheduled_event.get("t0", 0.0))
                except Exception:
                    t0 = 0.0

                if t0 > 0.0 and now_sec() >= t0 - self.prearm_before_t0:
                    return True

        return False

    def _state_requires_arm(self) -> bool:
        """
        判断当前任务状态是否需要自动解锁。

        注意：
            最终是否真的 arm，还要看：
                1. auto_arm；
                2. rc_offboard_permission；
                3. mavros_connected；
                4. 当前是否已经 armed。
        """
        if not self.enable_offboard_manager:
            return False

        if not self.auto_arm:
            return False

        if self._offboard_landing_active():
            return True

        if self.task_state in [
            self.TASK_TAKEOFF_RUNNING,
            self.TASK_WAIT_ENTER_FOLLOW_CMD,
            self.TASK_FOLLOW_MASTER,
            self.TASK_HOOK_SEQUENCE_RUNNING,
        ]:
            return True

        # 收到 takeoff 的 SCHEDULED 后，在 t0 前 prearm_before_t0 秒内提前解锁
        if self.scheduled_event is not None:
            action = self.scheduled_event.get("action", "")
            if action == "takeoff":
                try:
                    t0 = float(self.scheduled_event.get("t0", 0.0))
                except Exception:
                    t0 = 0.0

                if t0 > 0.0 and now_sec() >= t0 - self.prearm_before_t0:
                    return True

        return False

    def _is_return_px4_mode(self) -> bool:
        return self._normalize_px4_mode(self.mavros_mode) in self.return_px4_modes

    def _offboard_landing_active(self) -> bool:
        return self.task_state == self.TASK_LAND_RUNNING and self.use_offboard_landing

    def _should_skip_offboard_pos_requests(self) -> bool:
        px4_land_running = (
            self.task_state == self.TASK_LAND_RUNNING
            and not self.use_offboard_landing
        )
        return px4_land_running or self._is_return_px4_mode()

    def _manage_offboard_and_arm_low_rate(self):
        """
        低频管理 PX4 OFFBOARD 和 ARM。

        原则：
            1. FSM 不发布具体控制量；
            2. 控制量由外部控制节点持续发布；
            3. CH9 低位不请求任何模式；
            4. CH9 中位低频请求 POSCTL；
            5. CH9 高位才允许程序根据任务状态请求 OFFBOARD / ARM。
        """
        if not self.enable_offboard_manager:
            return

        t = now_sec()
        period = 1.0 / max(self.offboard_manage_rate, 1e-6)

        if t - self.last_offboard_manage_time < period:
            return

        self.last_offboard_manage_time = t

        need_offboard = self._state_requires_offboard()
        need_arm = self._state_requires_arm()

        if self._should_skip_offboard_pos_requests():
            if (
                self.task_state == self.TASK_LAND_RUNNING
                or self.rc_program_switch_mode == "pos"
                or need_offboard
            ):
                rospy.logwarn_throttle(
                    2.0,
                    "[%s TaskFSM] state=%s px4_mode=%s; skip OFFBOARD/POSCTL requests",
                    self.self_id,
                    self.task_state,
                    self.mavros_mode,
                )
            return

        # CH9 中位是 POS 模式：持续限频请求 POSCTL。
        # CH9 低位是 idle：不请求任何模式。
        # emergency 锁存后不允许这里覆盖紧急 HOLD。
        if (
            self.rc_program_switch_mode == "pos"
            and not self.cmd_emergency_hold
            and self.task_state != self.TASK_LAND_RUNNING
        ):
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] CH%s is in POS position; keep requesting POSCTL",
                self.self_id,
                self.offboard_switch_channel,
            )
            self._request_position_mode()

        if not need_offboard and not need_arm:
            return

        if not self._offboard_arm_request_delay_passed(t):
            return

        if not self.mavros_connected:
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] state=%s needs OFFBOARD/ARM, but MAVROS not connected",
                self.self_id,
                self.task_state,
            )
            return

        # 关键保护：普通任务中开关许可不满足时，程序绝不请求 OFFBOARD / ARM。
        # offboard 降落一旦同步开始，需要保持 OFFBOARD 才能继续跟踪下降轨迹。
        if not self.rc_offboard_permission:
            if self._offboard_landing_active():
                rospy.logwarn_throttle(
                    2.0,
                    "[%s TaskFSM] offboard landing active; keep requesting OFFBOARD even though switch source=%s does not permit program control",
                    self.self_id,
                    self.offboard_permission_source,
                )
            else:
                rospy.logwarn_throttle(
                    2.0,
                    "[%s TaskFSM] state=%s needs OFFBOARD/ARM, but switch source=%s does not permit program control",
                    self.self_id,
                    self.task_state,
                    self.offboard_permission_source,
                )
                return

        # 先请求 OFFBOARD
        if need_offboard and self.auto_offboard:
            if self.mavros_mode != "OFFBOARD":
                self._request_offboard_mode()
                return

        # 再请求 ARM
        if need_arm and self.auto_arm:
            if not self.mavros_armed:
                self._request_arm()

    def _offboard_arm_request_delay_passed(self, t: float) -> bool:
        delay = max(self.offboard_arm_request_delay, 0.0)
        if delay <= 0.0:
            return True

        base_time = self.state_enter_time
        if self.scheduled_event is not None:
            action = self.scheduled_event.get("action", "")
            if action == "takeoff":
                try:
                    t0 = float(self.scheduled_event.get("t0", 0.0))
                except Exception:
                    t0 = 0.0
                if t0 > 0.0:
                    base_time = t0 - self.prearm_before_t0

        if t < base_time + delay:
            rospy.loginfo_throttle(
                2.0,
                "[%s TaskFSM] stagger OFFBOARD/ARM request delay %.2fs",
                self.self_id,
                base_time + delay - t,
            )
            return False

        return True

    def _request_offboard_mode(self):
        """
        请求 PX4 进入 OFFBOARD。

        注意：
            PX4 进入 OFFBOARD 前，外部控制节点必须已经持续发布 setpoint。
            这个函数不负责 setpoint。
        """
        if self._should_skip_offboard_pos_requests():
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] skip OFFBOARD request in state=%s px4_mode=%s",
                self.self_id,
                self.task_state,
                self.mavros_mode,
            )
            return

        t = now_sec()

        if t - self.last_set_mode_req_time < self.set_mode_min_interval:
            return

        self.last_set_mode_req_time = t

        try:
            resp = self.set_mode_srv(custom_mode="OFFBOARD")

            rospy.logwarn(
                "[%s TaskFSM] request OFFBOARD, mode_sent=%s current_mode=%s",
                self.self_id,
                getattr(resp, "mode_sent", False),
                self.mavros_mode,
            )

        except rospy.ServiceException as e:
            rospy.logwarn(
                "[%s TaskFSM] set_mode OFFBOARD failed: %s",
                self.self_id,
                str(e),
            )

    def _request_position_mode(self):
        """
        请求 PX4 切回 POSCTL。
        """
        if self._offboard_landing_active():
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] offboard landing active; skip POSCTL request and keep OFFBOARD",
                self.self_id,
            )
            return

        if self._should_skip_offboard_pos_requests():
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] skip POSCTL request in state=%s px4_mode=%s",
                self.self_id,
                self.task_state,
                self.mavros_mode,
            )
            return

        if not self.mavros_connected:
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] cannot request POSCTL, MAVROS not connected",
                self.self_id,
            )
            return

        t = now_sec()
        if t - self.last_set_mode_req_time < self.set_mode_min_interval:
            return

        self.last_set_mode_req_time = t

        try:
            resp = self.set_mode_srv(custom_mode="POSCTL")

            rospy.logwarn(
                "[%s TaskFSM] request POSCTL, mode_sent=%s current_mode=%s",
                self.self_id,
                getattr(resp, "mode_sent", False),
                self.mavros_mode,
            )

        except rospy.ServiceException as e:
            rospy.logwarn(
                "[%s TaskFSM] set_mode POSCTL failed: %s",
                self.self_id,
                str(e),
            )

    def _ensure_master_pilot_hold_mode(self):
        if self.master_pilot_hold_mode_reached:
            return

        if not self.enable_master_pilot_hold_mode:
            self.master_pilot_hold_mode_reached = True
            return

        if not self.master_pilot_hold_px4_mode:
            self.master_pilot_hold_mode_reached = True
            return

        if self.mavros_mode == self.master_pilot_hold_px4_mode:
            self.master_pilot_hold_mode_reached = True
            rospy.logwarn(
                "[%s TaskFSM] master pilot hold mode reached: %s",
                self.self_id,
                self.master_pilot_hold_px4_mode,
            )
            return

        if not self.mavros_connected:
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] waiting MAVROS connection before requesting master pilot hold mode=%s",
                self.self_id,
                self.master_pilot_hold_px4_mode,
            )
            return

        t = now_sec()
        if t - self.last_set_mode_req_time < self.set_mode_min_interval:
            return

        self.last_set_mode_req_time = t

        try:
            resp = self.set_mode_srv(custom_mode=self.master_pilot_hold_px4_mode)
            rospy.logwarn(
                "[%s TaskFSM] request master pilot hold mode=%s, mode_sent=%s current_mode=%s",
                self.self_id,
                self.master_pilot_hold_px4_mode,
                getattr(resp, "mode_sent", False),
                self.mavros_mode,
            )
        except rospy.ServiceException as e:
            rospy.logwarn(
                "[%s TaskFSM] set_mode %s failed: %s",
                self.self_id,
                self.master_pilot_hold_px4_mode,
                str(e),
            )

    def _ensure_emergency_hold_mode(self):
        if self.emergency_hold_mode_reached:
            return

        if not self.enable_emergency_hold_mode:
            self.emergency_hold_mode_reached = True
            return

        if not self.emergency_hold_px4_mode:
            self.emergency_hold_mode_reached = True
            return

        if self.mavros_mode == self.emergency_hold_px4_mode:
            self.emergency_hold_mode_reached = True
            rospy.logwarn(
                "[%s TaskFSM] emergency hold mode reached: %s",
                self.self_id,
                self.emergency_hold_px4_mode,
            )
            return

        if not self.mavros_connected:
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] waiting MAVROS connection before requesting emergency hold mode=%s",
                self.self_id,
                self.emergency_hold_px4_mode,
            )
            return

        t = now_sec()
        if t - self.last_set_mode_req_time < self.set_mode_min_interval:
            return

        self.last_set_mode_req_time = t

        try:
            resp = self.set_mode_srv(custom_mode=self.emergency_hold_px4_mode)
            rospy.logwarn(
                "[%s TaskFSM] request emergency hold mode=%s, mode_sent=%s current_mode=%s",
                self.self_id,
                self.emergency_hold_px4_mode,
                getattr(resp, "mode_sent", False),
                self.mavros_mode,
            )
        except rospy.ServiceException as e:
            rospy.logwarn(
                "[%s TaskFSM] set_mode %s failed: %s",
                self.self_id,
                self.emergency_hold_px4_mode,
                str(e),
            )

    def _ensure_px4_land_mode(self):
        if not self.enable_px4_land_mode:
            return

        if not self.land_px4_mode:
            return

        if self.mavros_mode == self.land_px4_mode:
            return

        if not self.mavros_connected:
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] waiting MAVROS connection before requesting PX4 land mode=%s",
                self.self_id,
                self.land_px4_mode,
            )
            return

        t = now_sec()
        if t - self.last_set_mode_req_time < self.set_mode_min_interval:
            return

        self.last_set_mode_req_time = t

        try:
            resp = self.set_mode_srv(custom_mode=self.land_px4_mode)
            rospy.logwarn(
                "[%s TaskFSM] request PX4 land mode=%s, mode_sent=%s current_mode=%s",
                self.self_id,
                self.land_px4_mode,
                getattr(resp, "mode_sent", False),
                self.mavros_mode,
            )
        except rospy.ServiceException as e:
            rospy.logwarn(
                "[%s TaskFSM] set_mode %s failed: %s",
                self.self_id,
                self.land_px4_mode,
                str(e),
            )

    def _request_transfer_to_landing(self):
        if not self.transfer_to_landing_service:
            return

        try:
            rospy.wait_for_service(
                self.transfer_to_landing_service,
                timeout=self.transfer_to_landing_service_timeout,
            )
        except (rospy.ROSException, rospy.ServiceException) as exc:
            rospy.logerr(
                "[%s TaskFSM] transfer_to_landing service unavailable service=%s sync_id=%s error=%s",
                self.self_id,
                self.transfer_to_landing_service,
                self.current_sync_id,
                exc,
            )
            return

        request_data = {
            "src": self.self_id,
            "stamp": now_sec(),
            "action": "land",
            "mode": "TRAJECTORY_LANDING",
            "sync_id": self.current_sync_id,
            "t0": self.current_t0,
            "payload": self.current_payload,
        }
        request_payload = json.dumps(
            request_data,
            ensure_ascii=False,
            separators=(",", ":"),
        )

        result_queue = queue.Queue(maxsize=1)

        def _call_service():
            try:
                result_queue.put((True, self.transfer_to_landing_srv(request_payload)))
            except Exception as exc:
                result_queue.put((False, exc))

        call_thread = threading.Thread(target=_call_service)
        call_thread.daemon = True
        call_thread.start()

        try:
            ok, result = result_queue.get(
                timeout=self.transfer_to_landing_service_timeout,
            )
        except queue.Empty:
            rospy.logerr(
                "[%s TaskFSM] transfer_to_landing service no response within %.3fs service=%s sync_id=%s",
                self.self_id,
                self.transfer_to_landing_service_timeout,
                self.transfer_to_landing_service,
                self.current_sync_id,
            )
            return

        if not ok:
            rospy.logerr(
                "[%s TaskFSM] transfer_to_landing service call failed service=%s sync_id=%s error=%s",
                self.self_id,
                self.transfer_to_landing_service,
                self.current_sync_id,
                result,
            )
            return

        resp = result

        if bool(getattr(resp, "ok", False)):
            rospy.loginfo(
                "[%s TaskFSM] transfer_to_landing service accepted service=%s sync_id=%s t0=%.3f reason=%s payload=%s",
                self.self_id,
                self.transfer_to_landing_service,
                self.current_sync_id,
                self.current_t0,
                getattr(resp, "reason", ""),
                getattr(resp, "payload", ""),
            )
            return

        rospy.logerr(
            "[%s TaskFSM] transfer_to_landing service rejected service=%s sync_id=%s t0=%.3f reason=%s payload=%s",
            self.self_id,
            self.transfer_to_landing_service,
            self.current_sync_id,
            self.current_t0,
            getattr(resp, "reason", ""),
            getattr(resp, "payload", ""),
        )

    def _request_arm(self):
        """
        请求 PX4 解锁。
        """
        t = now_sec()

        if t - self.last_arm_req_time < self.arm_min_interval:
            return

        self.last_arm_req_time = t

        try:
            resp = self.arming_srv(True)

            rospy.logwarn(
                "[%s TaskFSM] request ARM, success=%s armed=%s",
                self.self_id,
                getattr(resp, "success", False),
                self.mavros_armed,
            )

        except rospy.ServiceException as e:
            rospy.logwarn(
                "[%s TaskFSM] arm failed: %s",
                self.self_id,
                str(e),
            )


    # ============================================================
    # 状态处理函数
    # ============================================================

    def _state_idle(self):
        """
        TASK_IDLE：
            master 等待外部 start。
            slave 一般不会停在这里。
        """
        # self.publish_controller_hold(capture_current=False)
        pass

    def _state_self_check(self):
        """
        TASK_SELF_CHECK：
            本机自检状态，不走同步器。

        你自己的自检可以写在 check_self_check_passed() 里，例如：
            - MAVROS 连接
            - 定位有效
            - 控制器在线
            - 轨迹规划器在线
            - 电池/燃电状态
            - 传感器状态
        """
        # self.publish_controller_hold(capture_current=False)
        self._call_trajectory_reset_service_if_needed()

        if self.check_self_check_passed():
            self._set_state(self.TASK_TAKEOFF_READY)

    def _call_trajectory_reset_service_if_needed(self):
        if self.trajectory_reset_success_in_self_check:
            return

        if not self.trajectory_reset_service:
            return

        t = now_sec()
        if (
            self.last_trajectory_reset_req_time > 0.0
            and t - self.last_trajectory_reset_req_time
            < self.trajectory_reset_retry_interval
        ):
            return

        self.last_trajectory_reset_req_time = t

        try:
            rospy.wait_for_service(
                self.trajectory_reset_service,
                timeout=self.trajectory_reset_service_timeout,
            )
            resp = rospy.ServiceProxy(self.trajectory_reset_service, Trigger)()
        except Exception as exc:
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] trajectory reset service call failed service=%s error=%s",
                self.self_id,
                self.trajectory_reset_service,
                exc,
            )
            return

        if bool(getattr(resp, "success", False)):
            self.trajectory_reset_success_in_self_check = True
            rospy.loginfo(
                "[%s TaskFSM] trajectory reset service succeeded: %s",
                self.self_id,
                getattr(resp, "message", ""),
            )
            return

        rospy.logwarn_throttle(
            2.0,
            "[%s TaskFSM] trajectory reset service returned false: %s",
            self.self_id,
            getattr(resp, "message", ""),
        )

    def _state_takeoff_ready(self):
        """
        TASK_TAKEOFF_READY：
            自检通过，准备同步起飞。

        这个状态是同步屏障状态：
            expected_action() 返回 "takeoff"。

        master：
            只有当前输入策略的 takeoff 上升沿触发后，
            才向 sync_gate_node 发起同步起飞 request。

        slave：
            不主动 request，只向 sync_gate_node 报告 ready_for_takeoff。
        """
        # self.publish_controller_hold(capture_current=False)

        if self.role == "master" and self._task_switch_takeoff_requested():
            self.cmd_takeoff = True
            if self._is_manual_control_strategy():
                rospy.logwarn(
                    "[%s TaskFSM] ManualControl takeoff bit %s triggered (buttons=%s)",
                    self.self_id,
                    self.manual_control_takeoff_bit,
                    self.manual_control_buttons,
                )
            else:
                rospy.logwarn(
                    "[%s TaskFSM] RC takeoff switch triggered on channel %s",
                    self.self_id,
                    self.rc_task_takeoff_channel,
                )

        if self.role == "master" and self.cmd_takeoff:
            self._master_request_sync_if_needed(
                action="takeoff",
                payload={
                    "height": self.takeoff_height,
                    "duration": self.takeoff_duration,
                    "planner": "vertical_takeoff_planner",
                    "controller": "velocity_tracking_controller",
                },
            )
        elif self.role == "master":
            rospy.loginfo_throttle(
                2.0,
                "[%s TaskFSM] waiting RC takeoff switch channel %s",
                self.self_id,
                self.rc_task_takeoff_channel,
            )

    def _state_takeoff_running(self):
        """
        TASK_TAKEOFF_RUNNING：
            同步触发后执行起飞。

        注意：
            起飞轨迹由独立轨迹规划器生成；
            控制器跟踪轨迹并通过 MAVROS 发 PX4 线速度指令。

        FSM 在这里主要做：
            1. 持续通知控制器处于 TRACK_TRAJECTORY；
            2. 判断是否到达指定高度；
            3. 到达后进入等待人工进入主从飞行阶段。
        """
        tau = now_sec() - self.current_t0

        # self.publish_controller_track_trajectory(
        #     trajectory_id=self.current_sync_id,
        #     action="takeoff",
        # )

        takeoff_reached = self.check_takeoff_reached(tau)

        if takeoff_reached and not self._task_switch_takeoff_advance_allowed():
            if self.role == "master":
                rospy.logwarn_throttle(
                    2.0,
                    "[%s TaskFSM] takeoff reached; waiting RC takeoff advance channel %s",
                    self.self_id,
                    self.rc_takeoff_advance_channel,
                )
            else:
                rospy.logwarn_throttle(
                    2.0,
                    "[%s TaskFSM] takeoff reached; waiting master takeoff advance command",
                    self.self_id,
                )
            return

        if takeoff_reached:
            self._finish_action()
            self._set_state(self.TASK_WAIT_ENTER_FOLLOW_CMD)
        else:
            rospy.loginfo_throttle(
                5.0,
                "[%s TaskFSM] taking off... tau=%.1f reached=%s",
                self.self_id,
                tau,
                takeoff_reached,
            )

    def _state_wait_enter_follow_cmd(self):
        """
        TASK_WAIT_ENTER_FOLLOW_CMD：
            已到达指定高度，等待人工指令进入主从飞行。

        不走同步器。
        人工通过 /operator/enter_follow 触发。

        master：
            进入 MASTER_PILOT_HOLD。
            默认是软件 HOLD，等待飞手接管/上层节点接管。
            如果你后面明确要切 PX4 HOLD，可以在 on_enter_master_pilot_hold() 里加 MAVROS set_mode。

        slave：
            进入 FOLLOW_MASTER。
        """
        # self.publish_controller_hold(capture_current=False)

        if self.cmd_enter_follow:
            if self.role == "master":
                self._set_state(self.TASK_MASTER_PILOT_HOLD)
            else:
                self._set_state(self.TASK_FOLLOW_MASTER)

    def _state_master_pilot_hold(self):
        """
        TASK_MASTER_PILOT_HOLD：
            master 等待或接受飞手接管。

        先保证 PX4 进入 HOLD；一旦确认进入过 HOLD，后续不再强切，
        让飞手可以通过遥控器覆盖接管。
        """
        self._ensure_master_pilot_hold_mode()

        if self._handle_follow_stage_actions(return_state=self.TASK_MASTER_PILOT_HOLD):
            return

    def _state_follow_master(self):
        """
        TASK_FOLLOW_MASTER：
            slave 从机跟随 master。

        不走同步器。
        你的从机跟随控制器在这里工作。
        """
        # self.publish_controller_follow_master()

        if self._handle_follow_stage_actions(return_state=self.TASK_FOLLOW_MASTER):
            return

        # slave 不主动 request land。
        # slave 在该状态下 expected_action() 会返回 "land"，
        # 这样 master 发起 land 同步时，slave 可以 ACK。
        pass

    def _state_hook_sequence_running(self):
        """
        TASK_HOOK_SEQUENCE_RUNNING：
            松钩，等待 5s，收绳。

        这个阶段飞行控制权保持不变：
            master 仍然保持 master pilot hold / 飞手控制；
            slave 仍然跟随 master。

        所以这里除了控制钩子/绳子，不改变飞控控制权。
        """
        if self.role == "master" and (
            self.cmd_land or self._task_switch_land_requested()
        ):
            self._master_request_sync_if_needed(
                action="land",
                payload={
                    "mode": (
                        "TRAJECTORY_LANDING"
                        if self.use_offboard_landing
                        else "PX4_LAND"
                    ),
                    "requested_by": "operator_during_hook",
                },
            )
            return

        # 保持原飞行控制模式
        if self.return_state_after_hook == self.TASK_MASTER_PILOT_HOLD:
            # self.publish_controller_master_pilot_hold()
            pass
        elif self.return_state_after_hook == self.TASK_FOLLOW_MASTER:
            # self.publish_controller_follow_master()
            pass
        else:
            # self.publish_controller_hold(capture_current=False)
            pass

        elapsed = now_sec() - self.hook_sequence_start_time

        if not self.hook_released:
            self.publish_hook_command(command="release_hook")
            self.hook_released = True
            rospy.loginfo("[%s TaskFSM] hook released", self.self_id)

        if elapsed >= self.hook_to_retract_delay and not self.rope_retracted:
            self.publish_hook_command(command="retract_rope")
            self.rope_retracted = True
            rospy.loginfo("[%s TaskFSM] rope retract command sent", self.self_id)

        # 这里假设发出收绳命令后就可以返回。
        # 如果你的收绳执行器有反馈，可以改成等待反馈。
        if self.hook_released and self.rope_retracted:
            self.cmd_hook_sequence = False
            self._set_state(self.return_state_after_hook)

    def _state_land_running(self):
        """
        TASK_LAND_RUNNING：
            同步触发后，所有飞机同时进入 LAND。

        offboard 降落由 transfer_to_landing 服务触发；PX4 降落则持续请求 AUTO.LAND。
        """
        if not self.use_offboard_landing:
            self._ensure_px4_land_mode()

        # self.publish_controller_land()

        if self.check_landed():
            self._set_state(self.TASK_RESETTING)

    def _state_resetting(self):
        """
        TASK_RESETTING：
            落地后复位，等待下一轮运行。
        """
        self._reset_runtime_flags()

        if self.role == "master":
            self._set_state(self.TASK_IDLE)
        else:
            self._set_state(self.TASK_SELF_CHECK)

    def _state_emergency_hold(self):
        """
        TASK_EMERGENCY_HOLD：
            紧急保持，持续请求 PX4 HOLD，直到模式真的切过去。
        """
        # self.publish_controller_hold(capture_current=False)
        self._ensure_emergency_hold_mode()

    def _state_aborted(self):
        """
        TASK_ABORTED：
            同步失败或任务异常，持续请求 PX4 HOLD，等待人工干预。
        """
        # self.publish_controller_hold(capture_current=False)
        self._ensure_emergency_hold_mode()

    def _state_unknown(self):
        rospy.logerr("[%s TaskFSM] unknown task_state=%s", self.self_id, self.task_state)
        self._set_state(self.TASK_EMERGENCY_HOLD)

    # ============================================================
    # 同步状态发布：TaskFSM -> SyncGate
    # ============================================================

    def _call_json_service(
        self,
        service_proxy,
        service_name: str,
        data: Dict[str, Any],
        label: str,
        handle_response_event: bool = False,
    ):
        payload_text = json.dumps(data, separators=(",", ":"))

        try:
            rospy.wait_for_service(service_name, timeout=self.sync_service_timeout)
            resp = service_proxy(payload_text)
        except (rospy.ROSException, rospy.ServiceException) as e:
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] %s service call failed name=%s err=%s",
                self.self_id,
                label,
                service_name,
                str(e),
            )
            return None

        if handle_response_event and resp.payload:
            event_data = safe_json_loads(resp.payload)
            if event_data:
                self._handle_sync_event(event_data)
            else:
                rospy.logwarn(
                    "[%s TaskFSM] %s service returned invalid event json: %s",
                    self.self_id,
                    label,
                    resp.payload,
                )

        if not resp.ok and not (handle_response_event and resp.payload):
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] %s service returned not ok name=%s reason=%s",
                self.self_id,
                label,
                service_name,
                resp.reason,
            )

        return resp

    def _publish_sync_status_periodically(self):
        """
        发布给 sync_gate_node 的同步状态。

        只有 expected_action() 非空时，ready=true。
        即：
            TASK_TAKEOFF_READY 可以 ready_for_takeoff；
            TASK_MASTER_PILOT_HOLD / TASK_FOLLOW_MASTER 可以 ready_for_land；
            其他本机状态不参与同步。
        """
        t = now_sec()
        period = 1.0 / max(self.status_rate, 1e-6)

        if t - self.last_status_pub_time < period:
            return

        self.last_status_pub_time = t

        expected = self.expected_action()
        ready = expected != ""

        data = {
            "src": self.self_id,
            "stamp": t,
            "task_state": self.task_state,
            "ready": ready,
            "expected_action": expected,
            "reason": f"ready_for_{expected}" if ready else self.task_state,
            "current_action": self.current_action,
            "current_sync_id": self.current_sync_id,
        }

        self._call_json_service(
            self.status_srv,
            self.task_status_topic,
            data,
            "sync_status",
        )

    def expected_action(self) -> str:
        """
        当前任务状态下允许同步器触发的 action。

        注意：
            这里不是所有状态都有 action。
            只有真正需要多机同步的阶段才返回非空字符串。
        """
        if self.task_state == self.TASK_TAKEOFF_READY:
            return "takeoff"

        # land 同步：
        # master 在 MASTER_PILOT_HOLD 状态下收到人工 land 指令后 request；
        # slave 在 FOLLOW_MASTER 状态下等待 master 的 land 同步。
        if self.task_state in [
            self.TASK_MASTER_PILOT_HOLD,
            self.TASK_FOLLOW_MASTER,
            self.TASK_HOOK_SEQUENCE_RUNNING,
        ]:
            return "land"

        return ""

    # ============================================================
    # master TaskFSM -> master SyncGate：同步申请
    # ============================================================

    def _master_request_sync_if_needed(self, action: str, payload: Dict[str, Any]):
        """
        master 主动发起同步。

        只应该用于：
            1. 同步起飞：takeoff
            2. 同步降落：land
        """
        if self.role != "master":
            return

        if self.request_in_flight:
            return

        if self.scheduled_event is not None:
            return

        t = now_sec()
        period = 1.0 / max(self.request_rate, 1e-6)

        if t - self.last_request_pub_time < period:
            return

        self.last_request_pub_time = t
        self.request_counter += 1

        request_id = f"{self.self_id}_{self.request_counter:04d}_{action}"

        data = {
            "src": self.self_id,
            "stamp": t,
            "request_id": request_id,
            "action": action,
            "payload": payload,
        }

        self._call_json_service(
            self.request_srv,
            self.task_request_topic,
            data,
            "sync_request",
            handle_response_event=True,
        )

        rospy.loginfo(
            "[master TaskFSM] call sync_request action=%s request_id=%s",
            action,
            request_id,
        )

    # ============================================================
    # SCHEDULED / START 触发同步动作
    # ============================================================

    def _start_scheduled_action_if_due(self):
        """
        收到 SCHEDULED 后，根据 t0 自己启动任务。

        原始逻辑：
            每次 FSM tick 判断 now >= t0，到了就启动。

        改进逻辑：
            1. 距离 t0 还远时，不阻塞，直接返回；
            2. 距离 t0 进入 precise_wait_window 秒以内时，短暂进入精细等待；
            3. 最后 busy_wait_window 秒以内不 sleep，busy wait，尽量贴近 t0 触发。

        注意：
            - 这里最多会在 t0 前阻塞 precise_wait_window 秒，默认 20ms；
            - 只适合临近同步触发时使用；
            - START 事件仍然作为兜底事件；
            - 同一个 sync_id 的重复启动由 _start_action_from_event() 里的 started_sync_ids 负责防重。
        """
        if self.scheduled_event is None:
            return

        # 先缓存事件，避免等待过程中 self.scheduled_event 被其他路径清空。
        event = self.scheduled_event

        action = event.get("action", "")
        expected = self.expected_action()

        # 当前状态已经不是等待这个 action 的状态，说明事件过期或状态已变化。
        if action != expected:
            return

        try:
            t0 = float(event.get("t0", 0.0))
        except Exception:
            return
        
        # 如果约定好起飞则发送起飞参数给轨迹规划器
        if event.get("action", "") == "takeoff" and self.send_count < self.max_send_count:
            self.send_count += 1
            payload = event.get("payload", {})
            if not isinstance(payload, dict):
                payload = {}

            sync_id = event.get("sync_id", "")
            action = event.get("action", "")
            t0 = float(event.get("t0", 0.0))

            data = {
                "action": action,
                "t0": t0,
                "height": float(payload.get("height", self.takeoff_height)),
                "duration": float(payload.get("duration", self.takeoff_duration)),
            }

            self.takeoff_param_pub.publish(
                String(data=json.dumps(data, ensure_ascii=False, separators=(",", ":")))
            )

        if t0 <= 0:
            return

        # 可以通过 launch 参数调整。
        # precise_wait_window: 距离 t0 多近时进入精细等待，默认 20ms。
        # busy_wait_window: 最后多长时间 busy wait，默认 2ms。
        # sleep_step: 精细等待阶段的短 sleep 步长，默认 0.5ms。
        precise_wait_window = float(rospy.get_param("~precise_wait_window", 0.020))
        busy_wait_window = float(rospy.get_param("~busy_wait_window", 0.002))
        sleep_step = float(rospy.get_param("~precise_wait_sleep_step", 0.0005))

        now = now_sec()
        remain = t0 - now

        # 离 t0 还远，不要阻塞主循环。
        if remain > precise_wait_window:
            return

        # 进入 t0 前的小时间窗，开始精细等待。
        while not rospy.is_shutdown():
            now = now_sec()
            remain = t0 - now

            if remain <= 0:
                break

            # 还剩 2ms 以上，用短 sleep，降低 CPU 占用。
            if remain > busy_wait_window:
                rospy.sleep(min(sleep_step, max(remain - busy_wait_window, 0.0)))
            else:
                # 最后 busy_wait_window 秒 busy wait。
                # 不 sleep，尽量减少调度误差。
                pass

        self._start_action_from_event(event)
        self.send_count = 0

    def _start_action_from_event(self, event: dict):
        """
        同步器触发后的 action -> task_state 映射。

        注意：
            同一个同步会话可能通过两条路径触发：
                1. SCHEDULED 事件保存 t0 后，FSM 主循环检测 now >= t0；
                2. SyncGate 到 t0 后发布 START 兜底事件。

            这两条路径在 t0 附近可能几乎同时到达。
            因此这里必须用 sync_id 做幂等保护，保证同一个同步会话只启动一次。
        """
        action = event.get("action", "")
        event_sync_id = event.get("sync_id", "")

        if not event_sync_id:
            rospy.logwarn(
                "[%s TaskFSM] ignore sync event without sync_id action=%s event=%s",
                self.self_id,
                action,
                event,
            )
            return

        # 防重复启动：
        # 如果 SCHEDULED 到点触发和 START 兜底事件都调用了本函数，
        # 第二次会被这里挡住，避免重复规划轨迹、重复发送 LAND、重复状态进入动作。
        if event_sync_id in self.started_sync_ids:
            rospy.logwarn(
                "[%s TaskFSM] ignore duplicated sync start action=%s sync_id=%s",
                self.self_id,
                action,
                event_sync_id,
            )
            return

        expected = self.expected_action()

        if action != expected:
            rospy.logwarn(
                "[%s TaskFSM] ignore sync event action=%s expected=%s state=%s sync_id=%s",
                self.self_id,
                action,
                expected,
                self.task_state,
                event_sync_id,
            )
            return

        # 通过 action/state 检查后，立刻登记为已启动。
        # 必须在真正执行 _on_synced_xxx_start() 之前登记，
        # 否则两个回调连续进入时仍可能重复执行一次性动作。
        self.started_sync_ids.add(event_sync_id)

        self.current_action = action
        self.current_payload = event.get("payload", {})
        if not isinstance(self.current_payload, dict):
            self.current_payload = {}

        self.current_sync_id = event.get("sync_id", "")
        self.current_t0 = float(event.get("t0", now_sec()))
        self.action_start_time = now_sec()

        self.scheduled_event = None
        self.request_in_flight = False

        rospy.loginfo(
            "[%s TaskFSM] START synced action=%s sync_id=%s t0=%.3f",
            self.self_id,
            action,
            self.current_sync_id,
            self.current_t0,
        )

        if action == "takeoff":
            self._on_synced_takeoff_start()

        elif action == "land":
            self._on_synced_land_start()

        else:
            rospy.logerr("[%s TaskFSM] unknown synced action=%s", self.self_id, action)
            self._set_state(self.TASK_ABORTED)

    def _on_synced_takeoff_start(self):
        """
        同步起飞开始。

        这里做一次性动作：
            1. 通知轨迹规划器规划原地起飞轨迹；
            2. 通知控制器进入轨迹跟踪模式；
            3. 切入 TASK_TAKEOFF_RUNNING。
        """
        self.takeoff_height = float(
            self.current_payload.get("height", self.takeoff_height)
        )
        self.takeoff_duration = float(
            self.current_payload.get("duration", self.takeoff_duration)
        )

        self.publish_planner_request(
            request_type="plan_vertical_takeoff",
            payload={
                "trajectory_id": self.current_sync_id,
                "t0": self.current_t0,
                "height": self.takeoff_height,
                "duration": self.takeoff_duration,
                "frame": "local",
            },
        )

        # self.publish_controller_track_trajectory(
        #     trajectory_id=self.current_sync_id,
        #     action="takeoff",
        # )

        self._set_state(self.TASK_TAKEOFF_RUNNING)

    def _on_synced_land_start(self):
        """
        同步降落开始。
        """
        self.land_start_time = now_sec()

        if self.use_offboard_landing:
            self._request_transfer_to_landing()
            self._set_state(self.TASK_LAND_RUNNING)
            self._request_offboard_mode()
            return
        else:
            self._ensure_px4_land_mode()
        # self.publish_controller_land()

        self._set_state(self.TASK_LAND_RUNNING)

    # ============================================================
    # 状态切换与复位
    # ============================================================

    def _set_state(self, new_state: str):
        if new_state == self.task_state:
            return

        old_state = self.task_state
        self.prev_task_state = old_state
        self.task_state = new_state
        self.state_enter_time = now_sec()

        rospy.loginfo(
            "[%s TaskFSM] %s -> %s",
            self.self_id,
            old_state,
            new_state,
        )

        self._on_enter_state(new_state, old_state)

    def _on_enter_state(self, new_state: str, old_state: str):
        """
        状态进入时的一次性动作。
        """
        if new_state == self.TASK_WAIT_ENTER_FOLLOW_CMD:
            if self.role == "master":
                self._master_call_enter_follow_services()

        elif new_state == self.TASK_SELF_CHECK:
            self.trajectory_reset_success_in_self_check = False
            self.last_trajectory_reset_req_time = 0.0

        elif new_state == self.TASK_MASTER_PILOT_HOLD:
            # 同一轮任务只保证切入一次 PX4 HOLD；hook 后回到本状态不再强切。
            # self.publish_controller_master_pilot_hold(capture_current=True)
            pass

        elif new_state == self.TASK_FOLLOW_MASTER:
            # self.publish_controller_follow_master()
            pass

        elif new_state == self.TASK_EMERGENCY_HOLD:
            self.emergency_hold_mode_reached = False
            # 紧急 HOLD：进入状态时捕获当前位置。
            # self.publish_controller_hold(capture_current=True)

        elif new_state == self.TASK_ABORTED:
            self.emergency_hold_mode_reached = False

        elif new_state == self.TASK_LAND_RUNNING:
            self.land_start_time = now_sec()

    def _master_call_enter_follow_services(self):
        target_ids = [self.self_id]
        for participant in self.participants:
            if participant not in target_ids:
                target_ids.append(participant)

        for target_id in target_ids:
            service_name = f"/{target_id}/operator/enter_follow"
            try:
                rospy.wait_for_service(
                    service_name,
                    timeout=self.operator_enter_follow_service_wait_timeout,
                )
                resp = rospy.ServiceProxy(service_name, Trigger)()
                if resp.success:
                    rospy.loginfo(
                        "[%s TaskFSM] enter_follow service ack from %s",
                        self.self_id,
                        target_id,
                    )
                else:
                    rospy.logwarn(
                        "[%s TaskFSM] enter_follow service rejected by %s: %s",
                        self.self_id,
                        target_id,
                        resp.message,
                    )
            except Exception as exc:
                rospy.logwarn(
                    "[%s TaskFSM] enter_follow service call failed target=%s service=%s error=%s",
                    self.self_id,
                    target_id,
                    service_name,
                    exc,
                )

    def _finish_action(self):
        """
        清理当前同步动作信息。

        不代表整个任务结束，只代表当前同步触发动作结束。
        """
        self.current_action = ""
        self.current_payload = {}
        self.current_sync_id = ""
        self.current_t0 = 0.0
        self.action_start_time = 0.0
        self.scheduled_event = None
        self.request_in_flight = False

    def _reset_runtime_flags(self):
        """
        落地后复位，为下一轮运行准备。
        """
        self.cmd_takeoff = False
        self.cmd_enter_follow = False
        self.cmd_hook_sequence = False
        self.cmd_land = False
        self.cmd_emergency_hold = False
        self.task_switch_takeoff_active_last = False
        self.task_switch_hook_active_last = False

        self._finish_action()

        # 一轮任务结束后清空已启动 sync_id 记录。
        # 新一轮任务会生成新的 sync_id；这里清空可以避免集合无限增长。
        self.started_sync_ids.clear()

        self.hook_sequence_start_time = 0.0
        self.hook_released = False
        self.rope_retracted = False
        self.return_state_after_hook = ""

        self.land_start_time = 0.0
        self.master_pilot_hold_mode_reached = False

    # ============================================================
    # 松钩/收绳流程
    # ============================================================

    def _begin_hook_sequence(self, return_state: str):
        """
        开始松钩/收绳流程。

        return_state 表示流程结束后回到哪里：
            master -> TASK_MASTER_PILOT_HOLD
            slave  -> TASK_FOLLOW_MASTER
        """
        self.return_state_after_hook = return_state
        self.hook_sequence_start_time = now_sec()
        self.hook_released = False
        self.rope_retracted = False
        self.cmd_hook_sequence = False

        self._set_state(self.TASK_HOOK_SEQUENCE_RUNNING)

    # ============================================================
    # 条件判断：你主要改这里
    # ============================================================

    def _is_fresh(self, stamp: float, timeout: float) -> bool:
        """
        判断某个 topic 是否在 timeout 内更新过。
        """
        return stamp > 0.0 and (now_sec() - stamp) <= timeout

    def _check_gps_ok(self) -> bool:
        """
        GPS 检查：
            1. GPS topic 新鲜；
            2. fix status >= gps_min_status；
            3. 经纬高为有限数。
        """
        if not self.require_gps:
            return True

        gps_fresh = self._is_fresh(self.last_gps_time, self.gps_timeout)

        gps_fix_ok = self.gps_fix_status >= self.gps_min_status

        gps_value_ok = (
            math.isfinite(self.gps_lat)
            and math.isfinite(self.gps_lon)
            and math.isfinite(self.gps_alt)
        )

        return gps_fresh and gps_fix_ok and gps_value_ok

    def _check_ekf_ok(self) -> bool:
        """
        EKF2 检查：
            1. estimator_status topic 新鲜；
            2. 必要 flags 为 True；
            3. 没有 gps_glitch / accel_error。
        """
        if not self.require_ekf:
            return True

        ekf_fresh = self._is_fresh(self.last_ekf_time, self.ekf_timeout)
        if not ekf_fresh:
            return False

        required_checks = []

        if self.require_ekf_attitude:
            required_checks.append(self.ekf_flags["attitude"])

        if self.require_ekf_vel_horiz:
            required_checks.append(self.ekf_flags["vel_horiz"])

        if self.require_ekf_vel_vert:
            required_checks.append(self.ekf_flags["vel_vert"])

        if self.require_ekf_pos_horiz_abs:
            required_checks.append(self.ekf_flags["pos_horiz_abs"])

        if self.require_ekf_pos_vert_abs:
            required_checks.append(self.ekf_flags["pos_vert_abs"])

        if required_checks and not all(required_checks):
            return False

        if self.reject_ekf_gps_glitch and self.ekf_flags["gps_glitch"]:
            return False

        if self.reject_ekf_accel_error and self.ekf_flags["accel_error"]:
            return False

        return True

    def _check_rc_ok(self) -> bool:
        """
        遥控器检查：
            1. /mavros/rc/in topic 新鲜；
            2. 通道数足够；
            3. 如果配置了 rc_min_rssi，则 rssi 达标。
        """
        if not self.require_rc:
            return True

        rc_fresh = self._is_fresh(self.last_rc_time, self.rc_timeout)

        channels_ok = len(self.rc_channels) >= self.rc_min_channels

        if self.rc_min_rssi >= 0:
            rssi_ok = self.rc_rssi >= self.rc_min_rssi
        else:
            rssi_ok = True

        return rc_fresh and channels_ok and rssi_ok

    def _check_mavros_ok(self) -> bool:
        """
        MAVROS 连接检查。
        """
        if not self.require_mavros_connected:
            return True

        state_fresh = self._is_fresh(
            self.last_mavros_state_time,
            self.mavros_state_timeout,
        )

        return state_fresh and self.mavros_connected

    def _build_self_check_report(self) -> Dict[str, Any]:
        """
        生成自检报告，方便日志和 /mission/state 查看。
        """
        gps_fresh = self._is_fresh(self.last_gps_time, self.gps_timeout)
        ekf_fresh = self._is_fresh(self.last_ekf_time, self.ekf_timeout)
        rc_fresh = self._is_fresh(self.last_rc_time, self.rc_timeout)
        mavros_fresh = self._is_fresh(
            self.last_mavros_state_time,
            self.mavros_state_timeout,
        )

        gps_ok = self._check_gps_ok()
        ekf_ok = self._check_ekf_ok()
        rc_ok = self._check_rc_ok()
        mavros_ok = self._check_mavros_ok()

        report = {
            "bypass": self.self_check_bypass,
            "mavros": {
                "required": self.require_mavros_connected,
                "fresh": mavros_fresh,
                "connected": self.mavros_connected,
                "mode": self.mavros_mode,
                "armed": self.mavros_armed,
                "ok": mavros_ok,
            },
            "gps": {
                "required": self.require_gps,
                "fresh": gps_fresh,
                "fix_status": self.gps_fix_status,
                "min_status": self.gps_min_status,
                "lat": self.gps_lat,
                "lon": self.gps_lon,
                "alt": self.gps_alt,
                "ok": gps_ok,
            },
            "ekf": {
                "required": self.require_ekf,
                "fresh": ekf_fresh,
                "flags": dict(self.ekf_flags),
                "ok": ekf_ok,
            },
            "rc": {
                "required": self.require_rc,
                "fresh": rc_fresh,
                "channels_count": len(self.rc_channels),
                "min_channels": self.rc_min_channels,
                "rssi": self.rc_rssi,
                "min_rssi": self.rc_min_rssi,
                "ok": rc_ok,
            },
        }

        report["ok"] = (
            mavros_ok
            and gps_ok
            and ekf_ok
            and rc_ok
        )

        return report

    def check_self_check_passed(self) -> bool:
        """
        自检是否通过。

        当前检查：
            1. MAVROS 是否连接；
            2. GPS 是否有 fix；
            3. EKF2 状态是否正常；
            4. 是否有遥控器接入。

        注意：
            demo_mode=True 时，self_check_bypass 默认也是 True。
            实飞时请设置：
                demo_mode:=false
                self_check_bypass:=false
        """
        if self.self_check_bypass:
            self.self_check_report = {
                "ok": True,
                "bypass": True,
                "reason": "self_check_bypass_enabled",
            }
            return True

        self.self_check_report = self._build_self_check_report()
        ok = bool(self.self_check_report.get("ok", False))

        t = now_sec()
        if not ok and t - self.last_self_check_log_time >= self.self_check_log_period:
            self.last_self_check_log_time = t
            rospy.logwarn(
                "[%s TaskFSM] self check not passed: %s",
                self.self_id,
                json.dumps(
                    self.self_check_report,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            )

        if ok:
            self.last_self_check_log_time = t
            rospy.loginfo(
                "[%s TaskFSM] self check passed: %s",
                self.self_id,
                json.dumps(
                    self.self_check_report,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            )

        return ok

    def check_takeoff_reached(self, tau: float) -> bool:
        """
        是否到达指定高度。

        推荐真实逻辑：
            abs(current_z - target_z) < tolerance
            且垂向速度较小
            且持续满足一段时间
        """
        if self.demo_mode:
            return tau >= self.takeoff_duration

        # TODO: 替换为真实高度判断
        return tau >= self.takeoff_duration

    def check_landed(self) -> bool:
        """
        是否落地完成。

        推荐真实逻辑：
            - PX4 landed_state
        """
        if self.demo_mode:
            return now_sec() - self.land_start_time >= self.land_timeout

        t = now_sec()
        if (
            self.last_extended_state_time <= 0.0
            or t - self.last_extended_state_time > self.mavros_state_timeout
        ):
            rospy.logwarn_throttle(
                2.0,
                "[%s TaskFSM] waiting fresh MAVROS extended_state for landed detection",
                self.self_id,
            )
            return False

        return self.landed_state == int(
            getattr(ExtendedState, "LANDED_STATE_ON_GROUND", 1)
        )

    # ============================================================
    # 对外发布：轨迹规划器 / 控制器 / 钩子
    # ============================================================

    def publish_planner_request(self, request_type: str, payload: Dict[str, Any]):
        data = {
            "src": self.self_id,
            "stamp": now_sec(),
            "request_type": request_type,
            "payload": payload,
        }

        self.planner_request_pub.publish(
            String(data=json.dumps(data, separators=(",", ":")))
        )

    def publish_controller_command(self, mode: str, payload: Optional[Dict[str, Any]] = None):
        data = {
            "src": self.self_id,
            "stamp": now_sec(),
            "mode": mode,
            "payload": payload if isinstance(payload, dict) else {},
            "task_state": self.task_state,
        }

        self.controller_command_pub.publish(
            String(data=json.dumps(data, separators=(",", ":")))
        )

    def publish_controller_hold(self, capture_current: bool = False):
        """
        软件 HOLD。

        建议控制器逻辑：
            capture_current=True 时，控制器记录当前位姿作为 hold setpoint；
            之后持续以 capture_current=False 保持该 setpoint；
            控制器继续通过 MAVROS 发布速度/位置保持指令；
            不切 PX4 HOLD。
        """
        self.publish_controller_command(
            mode="SOFTWARE_HOLD",
            payload={
                "capture_current": capture_current,
                "use_px4_hold": False,
            },
        )

    def publish_controller_master_pilot_hold(self, capture_current: bool = False):
        """
        master 等待飞手接管/飞手控制阶段。

        默认仍然不直接交给 PX4 HOLD。
        你后面可以让控制器根据 RC 状态切换：
            - SOFTWARE_HOLD
            - PILOT_PASS_THROUGH
        """
        self.publish_controller_command(
            mode="MASTER_PILOT_HOLD",
            payload={
                "capture_current": capture_current,
                "use_px4_hold": False,
                "allow_pilot_takeover": True,
            },
        )

    def publish_controller_track_trajectory(self, trajectory_id: str, action: str):
        """
        控制器进入轨迹跟踪模式。

        控制器应该根据 trajectory_id 从轨迹规划器获取轨迹，
        然后通过 MAVROS 发 PX4 线速度指令。
        """
        self.publish_controller_command(
            mode="TRACK_TRAJECTORY",
            payload={
                "trajectory_id": trajectory_id,
                "action": action,
                "t0": self.current_t0,
            },
        )

    def publish_controller_follow_master(self):
        """
        从机跟随 master。

        具体跟随控制由你的独立控制器实现。
        """
        self.publish_controller_command(
            mode="FOLLOW_MASTER",
            payload={
                "master_id": "master",
                "keep_current_relative_offset": True,
            },
        )

    def publish_controller_land(self):
        """
        同步降落。

        这里不直接写死 PX4 LAND。
        可以由你的控制器或 mode manager 决定：
            - 调 MAVROS set_mode LAND
            - 或执行受控降落轨迹
        """
        self.publish_controller_command(
            mode="LAND",
            payload={
                "use_px4_land": True,
                "requested_by_sync": True,
            },
        )

    def publish_hook_command(self, command: str):
        data = {
            "src": self.self_id,
            "stamp": now_sec(),
            "command": command,
            "task_state": self.task_state,
        }

        self.hook_command_pub.publish(
            String(data=json.dumps(data, separators=(",", ":")))
        )

    # ============================================================
    # 状态发布给外部模块
    # ============================================================

    def _publish_mission_state_periodically(self):
        t = now_sec()
        period = 1.0 / max(self.mission_state_rate, 1e-6)

        if t - self.last_mission_state_pub_time < period:
            return

        self.last_mission_state_pub_time = t
        rc_takeoff_advance_value = None
        rc_takeoff_advance_active = None
        if self.role == "master":
            rc_takeoff_advance_value = self._rc_channel_value(
                self.rc_takeoff_advance_channel
            )
            rc_takeoff_advance_active = self._rc_task_switch_takeoff_advance_allowed()

        data = {
            "src": self.self_id,
            "stamp": t,
            "role": self.role,
            "task_state": self.task_state,
            "expected_sync_action": self.expected_action(),
            "current_action": self.current_action,
            "current_sync_id": self.current_sync_id,
            "current_t0": self.current_t0,
            "request_in_flight": self.request_in_flight,
            "scheduled": self.scheduled_event is not None,
            "return_mode_active": self._is_return_px4_mode(),
            "hook_released": self.hook_released,
            "rope_retracted": self.rope_retracted,
            "emergency_hold": self.task_state == self.TASK_EMERGENCY_HOLD,
            "self_check": self.self_check_report,
            "rc_offboard_permission": self.rc_offboard_permission,
            "offboard_permission_source": self.offboard_permission_source,
            "offboard_switch_channel": self.offboard_switch_channel,
            "offboard_switch_value": self._rc_channel_value(self.offboard_switch_channel),
            "offboard_switch_mode": self.rc_program_switch_mode,
            "offboard_switch_pos_threshold": self.offboard_switch_pos_threshold,
            "offboard_switch_high_threshold": self.offboard_switch_high_threshold,
            "rc_task_takeoff_channel": self.rc_task_takeoff_channel,
            "rc_task_takeoff_value": self._rc_channel_value(self.rc_task_takeoff_channel),
            "rc_task_takeoff_active": self._rc_task_switch_takeoff_requested(),
            "rc_takeoff_advance_channel": self.rc_takeoff_advance_channel,
            "rc_takeoff_advance_value": rc_takeoff_advance_value,
            "rc_takeoff_advance_active": rc_takeoff_advance_active,
            "takeoff_advance_source": (
                "local_rc_or_manual_control"
                if self.role == "master"
                else "master_enter_follow_service"
            ),
            "takeoff_advance_commanded": self.cmd_enter_follow,
            "rc_task_hook_channel": self.rc_task_hook_channel,
            "rc_task_hook_value": self._rc_channel_value(self.rc_task_hook_channel),
            "rc_task_hook_active": self._rc_task_switch_hook_requested(),
            "rc_emergency_hold_channel": self.rc_emergency_hold_channel,
            "rc_emergency_hold_value": self._rc_channel_value(self.rc_emergency_hold_channel),
            "rc_emergency_hold_active": self._rc_task_switch_emergency_hold_active(),
            "rc_task_land_channel": self.rc_task_land_channel,
            "rc_task_land_value": self._rc_channel_value(self.rc_task_land_channel),
            "rc_task_land_active": self._rc_task_switch_land_requested(),
            "manual_control_buttons": self.manual_control_buttons,
            "manual_control_sb_bits": "{}{}".format(
                self._manual_control_bit(self.manual_control_sb_high_bit),
                self._manual_control_bit(self.manual_control_sb_low_bit),
            ),
            "manual_control_offboard_bit": self.manual_control_offboard_bit,
            "manual_control_offboard": self._manual_control_bit(
                self.manual_control_offboard_bit
            ),
            "manual_control_takeoff_bit": self.manual_control_takeoff_bit,
            "manual_control_takeoff": self._manual_control_bit(
                self.manual_control_takeoff_bit
            ),
            "manual_control_sa": self._manual_control_bit(self.manual_control_sa_bit),
            "manual_control_sd": self._manual_control_bit(self.manual_control_sd_bit),
            "need_offboard": self._state_requires_offboard(),
            "need_arm": self._state_requires_arm(),
        }

        self.mission_state_pub.publish(
            String(data=json.dumps(data, separators=(",", ":")))
        )


def main():
    rospy.init_node("task_fsm_node")
    node = TaskFSMNode()
    node.spin()


if __name__ == "__main__":
    main()
