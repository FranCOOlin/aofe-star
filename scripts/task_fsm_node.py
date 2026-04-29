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
    4. 只通过本机 topic 和 sync_gate_node 交互。
    5. 不是所有状态切换都需要同步器。
       只有“多机必须同时进入”的动作才通过同步器。

本机同步接口：
    task_fsm_node -> sync_gate_node:
        /<self_id>/task/sync_status
        /<self_id>/task/sync_request

    sync_gate_node -> task_fsm_node:
        /<self_id>/task/sync_event

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
from typing import Any, Dict, Optional, Callable

import rospy
from std_msgs.msg import String, Bool


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

        if self.role not in ["master", "slave"]:
            raise RuntimeError("~role must be master or slave")

        # ========================================================
        # 本机同步接口 topic
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
        self.auto_start = bool(rospy.get_param("~auto_start", False))

        # ========================================================
        # 任务参数
        # ========================================================

        self.takeoff_height = float(rospy.get_param("~takeoff_height", 3.0))
        self.takeoff_duration = float(rospy.get_param("~takeoff_duration", 6.0))
        self.takeoff_reached_tolerance = float(
            rospy.get_param("~takeoff_reached_tolerance", 0.25)
        )

        self.hook_to_retract_delay = float(
            rospy.get_param("~hook_to_retract_delay", 5.0)
        )

        self.land_timeout = float(rospy.get_param("~land_timeout", 30.0))

        # demo 模式：
        # 如果没有接真实高度、落地检测，可以先用时间自动通过部分判断。
        self.demo_mode = bool(rospy.get_param("~demo_mode", True))

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

        # 当前同步动作信息
        self.current_action = ""
        self.current_payload: Dict[str, Any] = {}
        self.current_sync_id = ""
        self.current_t0 = 0.0
        self.action_start_wall_time = 0.0

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
        # ROS pub/sub
        # ========================================================

        self.status_pub = rospy.Publisher(
            self.task_status_topic,
            String,
            queue_size=50,
        )

        self.request_pub = rospy.Publisher(
            self.task_request_topic,
            String,
            queue_size=20,
        )

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

        rospy.Subscriber(
            self.task_event_topic,
            String,
            self._sync_event_cb,
            queue_size=50,
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

        rospy.loginfo(
            "[TaskFSMNode] role=%s self_id=%s init_state=%s",
            self.role,
            self.self_id,
            self.task_state,
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
        人工起飞指令。

        只由 master 使用：
            master 收到后，在 TASK_TAKEOFF_READY 状态下发起同步起飞。
        """
        if msg.data:
            self.cmd_takeoff = True

    def _operator_enter_follow_cb(self, msg: Bool):
        """
        起飞到指定高度后，人工确认进入主从飞行阶段。

        不走同步器。
        master：进入 MASTER_PILOT_HOLD。
        slave ：进入 FOLLOW_MASTER。
        """
        if msg.data:
            self.cmd_enter_follow = True

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

    def _sync_event_cb(self, msg: String):
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
        data = safe_json_loads(msg.data)
        if not data:
            return

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

            # 向 sync_gate_node 发布同步状态。
            self._publish_sync_status_periodically()

            # 向其他模块发布任务状态，方便轨迹规划器、控制器、监控节点使用。
            self._publish_mission_state_periodically()

            # 收到 SCHEDULED 后，任务层自己按 t0 启动。
            self._start_scheduled_action_if_due()

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
        if not self.cmd_emergency_hold:
            return

        if self.task_state != self.TASK_EMERGENCY_HOLD:
            self.request_in_flight = False
            self.scheduled_event = None
            self._set_state(self.TASK_EMERGENCY_HOLD)

    # ============================================================
    # 状态处理函数
    # ============================================================

    def _state_idle(self):
        """
        TASK_IDLE：
            master 等待外部 start。
            slave 一般不会停在这里。
        """
        self.publish_controller_hold(capture_current=False)

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
        self.publish_controller_hold(capture_current=False)

        if self.check_self_check_passed():
            self._set_state(self.TASK_TAKEOFF_READY)

    def _state_takeoff_ready(self):
        """
        TASK_TAKEOFF_READY：
            自检通过，准备同步起飞。

        这个状态是同步屏障状态：
            expected_action() 返回 "takeoff"。

        master：
            只有收到人工起飞指令 /operator/takeoff 后，
            才向 sync_gate_node 发起同步起飞 request。

        slave：
            不主动 request，只向 sync_gate_node 报告 ready_for_takeoff。
        """
        self.publish_controller_hold(capture_current=False)

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

        self.publish_controller_track_trajectory(
            trajectory_id=self.current_sync_id,
            action="takeoff",
        )

        if self.check_takeoff_reached(tau):
            self._finish_action()
            self._set_state(self.TASK_WAIT_ENTER_FOLLOW_CMD)

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
        self.publish_controller_hold(capture_current=False)

        if self.cmd_enter_follow:
            if self.role == "master":
                self._set_state(self.TASK_MASTER_PILOT_HOLD)
            else:
                self._set_state(self.TASK_FOLLOW_MASTER)

    def _state_master_pilot_hold(self):
        """
        TASK_MASTER_PILOT_HOLD：
            master 等待或接受飞手接管。

        默认不切 PX4 HOLD，而是发布软件 HOLD 给控制器。
        如果飞手已经通过遥控器接入，你可以让控制器进入 PILOT_PASS_THROUGH。
        这里留接口，不强行实现。
        """
        self.publish_controller_master_pilot_hold()

        # 松钩/收绳不需要同步。
        if self.cmd_hook_sequence:
            self._begin_hook_sequence(return_state=self.TASK_MASTER_PILOT_HOLD)
            return

        # 同步降落需要同步器。
        if self.cmd_land:
            self._master_request_sync_if_needed(
                action="land",
                payload={
                    "mode": "PX4_LAND_OR_CONTROLLER_LAND",
                    "requested_by": "operator",
                },
            )

    def _state_follow_master(self):
        """
        TASK_FOLLOW_MASTER：
            slave 从机跟随 master。

        不走同步器。
        你的从机跟随控制器在这里工作。
        """
        self.publish_controller_follow_master()

        # 松钩/收绳不需要同步。
        if self.cmd_hook_sequence:
            self._begin_hook_sequence(return_state=self.TASK_FOLLOW_MASTER)
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
        # 保持原飞行控制模式
        if self.return_state_after_hook == self.TASK_MASTER_PILOT_HOLD:
            self.publish_controller_master_pilot_hold()
        elif self.return_state_after_hook == self.TASK_FOLLOW_MASTER:
            self.publish_controller_follow_master()
        else:
            self.publish_controller_hold(capture_current=False)

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

        这里可以有两种实现：
            1. 发布指令给外部模式管理器，让它调用 MAVROS set_mode LAND；
            2. 发布给控制器，由控制器执行受控降落。

        这里留成统一 controller command。
        """
        self.publish_controller_land()

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
            紧急软件保持。

        你的偏好是不希望交还给 PX4。
        所以这里建议：
            1. 不切 PX4 HOLD；
            2. 控制器捕获当前位姿作为 hold setpoint；
            3. 持续通过 MAVROS 发速度/位置保持指令；
            4. 若后续需要恢复，由人工发 reset 或重新启动流程。
        """
        self.publish_controller_hold(capture_current=False)

    def _state_aborted(self):
        """
        TASK_ABORTED：
            同步失败或任务异常。
        """
        self.publish_controller_hold(capture_current=False)

    def _state_unknown(self):
        rospy.logerr("[%s TaskFSM] unknown task_state=%s", self.self_id, self.task_state)
        self._set_state(self.TASK_EMERGENCY_HOLD)

    # ============================================================
    # 同步状态发布：TaskFSM -> SyncGate
    # ============================================================

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

        self.status_pub.publish(
            String(data=json.dumps(data, separators=(",", ":")))
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

        self.request_pub.publish(
            String(data=json.dumps(data, separators=(",", ":")))
        )

        rospy.loginfo(
            "[master TaskFSM] publish sync_request action=%s request_id=%s",
            action,
            request_id,
        )

    # ============================================================
    # SCHEDULED / START 触发同步动作
    # ============================================================

    def _start_scheduled_action_if_due(self):
        """
        收到 SCHEDULED 后，根据 t0 自己启动任务。

        这样比等待 START 消息更准。
        START 仍然作为兜底事件存在。
        """
        if self.scheduled_event is None:
            return

        action = self.scheduled_event.get("action", "")
        expected = self.expected_action()

        if action != expected:
            return

        t0 = float(self.scheduled_event.get("t0", 0.0))

        if t0 <= 0:
            return

        if now_sec() >= t0:
            self._start_action_from_event(self.scheduled_event)

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
        self.action_start_wall_time = now_sec()

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

        self.publish_controller_track_trajectory(
            trajectory_id=self.current_sync_id,
            action="takeoff",
        )

        self._set_state(self.TASK_TAKEOFF_RUNNING)

    def _on_synced_land_start(self):
        """
        同步降落开始。
        """
        self.land_start_time = now_sec()

        self.publish_controller_land()

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
        if new_state == self.TASK_MASTER_PILOT_HOLD:
            # 默认使用软件 hold。
            # 如果你明确要切 PX4 HOLD，可以在这里调用独立 mode manager。
            self.publish_controller_master_pilot_hold(capture_current=True)

        elif new_state == self.TASK_FOLLOW_MASTER:
            self.publish_controller_follow_master()

        elif new_state == self.TASK_EMERGENCY_HOLD:
            # 紧急软件 HOLD：进入状态时捕获当前位置。
            self.publish_controller_hold(capture_current=True)

        elif new_state == self.TASK_LAND_RUNNING:
            self.land_start_time = now_sec()

    def _finish_action(self):
        """
        清理当前同步动作信息。

        不代表整个任务结束，只代表当前同步触发动作结束。
        """
        self.current_action = ""
        self.current_payload = {}
        self.current_sync_id = ""
        self.current_t0 = 0.0
        self.action_start_wall_time = 0.0
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

        self._finish_action()

        # 一轮任务结束后清空已启动 sync_id 记录。
        # 新一轮任务会生成新的 sync_id；这里清空可以避免集合无限增长。
        self.started_sync_ids.clear()

        self.hook_sequence_start_time = 0.0
        self.hook_released = False
        self.rope_retracted = False
        self.return_state_after_hook = ""

        self.land_start_time = 0.0

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

    def check_self_check_passed(self) -> bool:
        """
        自检是否通过。

        你可以在这里检查：
            - MAVROS 是否连接
            - 飞控状态是否正常
            - 定位是否有效
            - 控制器节点是否在线
            - 轨迹规划器是否在线
            - 电池/氢电系统是否正常
            - 钩子/绞盘是否正常
        """
        if self.demo_mode:
            return True

        # TODO: 替换为真实自检逻辑
        return False

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
        return False

    def check_landed(self) -> bool:
        """
        是否落地完成。

        推荐真实逻辑：
            - PX4 landed_state
            - 高度接近地面
            - 垂向速度接近 0
            - 电机状态/arming 状态
        """
        if self.demo_mode:
            return now_sec() - self.land_start_time >= self.land_timeout

        # TODO: 替换为真实落地判断
        return False

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
            "hook_released": self.hook_released,
            "rope_retracted": self.rope_retracted,
            "emergency_hold": self.task_state == self.TASK_EMERGENCY_HOLD,
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