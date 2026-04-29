#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
task_fsm_node.py

功能：
    示例任务状态机节点。

设计边界：
    1. 只负责任务状态。
    2. 不包含 PREPARE / COMMIT / ACK 等同步协议逻辑。
    3. 不直接调用 sync_gate_node 对象。
    4. 只通过本机 topic 与 sync_gate_node 通信。

本机通讯：
    task_fsm_node -> sync_gate_node:
        /<self_id>/task/sync_status
        /<self_id>/task/sync_request

    sync_gate_node -> task_fsm_node:
        /<self_id>/task/sync_event
"""

import json
from typing import Any, Dict, Optional

import rospy
from std_msgs.msg import String, Bool


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
    示例任务流程：

        TASK_IDLE
            ↓ start
        TASK_READY_FOR_TAKEOFF
            ↓ sync action = takeoff
        TASK_TAKEOFF_RUNNING
            ↓
        TASK_READY_FOR_MISSION_SWITCH
            ↓ sync action = switch_mission
        TASK_MISSION_SWITCHING
            ↓
        TASK_READY_FOR_TRANSPORT_CONTROL
            ↓ sync action = start_transport_control
        TASK_TRANSPORT_CONTROL_RUNNING
            ↓
        TASK_READY_FOR_RELEASE
            ↓ sync action = release_load
        TASK_RELEASE_RUNNING
            ↓
        TASK_FINISHED
    """

    TASK_IDLE = "TASK_IDLE"

    TASK_READY_FOR_TAKEOFF = "TASK_READY_FOR_TAKEOFF"
    TASK_TAKEOFF_RUNNING = "TASK_TAKEOFF_RUNNING"

    TASK_READY_FOR_MISSION_SWITCH = "TASK_READY_FOR_MISSION_SWITCH"
    TASK_MISSION_SWITCHING = "TASK_MISSION_SWITCHING"

    TASK_READY_FOR_TRANSPORT_CONTROL = "TASK_READY_FOR_TRANSPORT_CONTROL"
    TASK_TRANSPORT_CONTROL_RUNNING = "TASK_TRANSPORT_CONTROL_RUNNING"

    TASK_READY_FOR_RELEASE = "TASK_READY_FOR_RELEASE"
    TASK_RELEASE_RUNNING = "TASK_RELEASE_RUNNING"

    TASK_FINISHED = "TASK_FINISHED"
    TASK_ABORTED = "TASK_ABORTED"

    def __init__(self):
        self.role = rospy.get_param("~role", "slave").lower().strip()
        self.self_id = rospy.get_param("~self_id", "uav1")

        if self.role not in ["master", "slave"]:
            raise RuntimeError("~role must be master or slave")

        # 本机 topic。
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
        self.start_topic = rospy.get_param(
            "~start_topic",
            f"/{self.self_id}/task/start",
        )

        self.status_rate = float(rospy.get_param("~task_status_rate", 20.0))
        self.request_rate = float(rospy.get_param("~task_request_rate", 1.0))
        self.control_rate_hz = float(rospy.get_param("~control_rate_hz", 50.0))

        self.auto_start = bool(rospy.get_param("~auto_start", False))

        # 示例任务参数。
        self.takeoff_height = float(rospy.get_param("~takeoff_height", 3.0))
        self.takeoff_duration = float(rospy.get_param("~takeoff_duration", 6.0))
        self.switch_mission_duration = float(rospy.get_param("~switch_mission_duration", 2.0))
        self.transport_duration = float(rospy.get_param("~transport_duration", 8.0))
        self.release_duration = float(rospy.get_param("~release_duration", 2.0))

        # master 等待外部 start。
        # slave 默认进入第一阶段 ready。
        if self.role == "master":
            self.task_state = self.TASK_IDLE
        else:
            self.task_state = self.TASK_READY_FOR_TAKEOFF

        self.current_action = ""
        self.current_payload: Dict[str, Any] = {}
        self.current_sync_id = ""
        self.current_t0 = 0.0

        self.action_start_wall_time = 0.0

        # 收到 SCHEDULED 后保存。
        # 这不是同步状态，只是任务层保存的调度信息。
        self.scheduled_event: Optional[dict] = None

        # master 防止重复 request。
        self.request_in_flight = False
        self.last_request_pub_time = 0.0
        self.request_counter = 0

        self.last_status_pub_time = 0.0

        self.status_pub = rospy.Publisher(self.task_status_topic, String, queue_size=50)
        self.request_pub = rospy.Publisher(self.task_request_topic, String, queue_size=20)

        rospy.Subscriber(self.task_event_topic, String, self._event_cb, queue_size=50)
        rospy.Subscriber(self.start_topic, Bool, self._start_cb, queue_size=5)

        rospy.loginfo(
            "[TaskFSMNode] role=%s self_id=%s state=%s",
            self.role,
            self.self_id,
            self.task_state,
        )

    # ============================================================
    # 外部启动
    # ============================================================

    def _start_cb(self, msg: Bool):
        if msg.data:
            self.start()

    def start(self):
        """
        启动 master 任务流程。

        slave 不主动启动，只等待 master 发起同步。
        """
        if self.role != "master":
            return

        if self.task_state == self.TASK_IDLE:
            self.task_state = self.TASK_READY_FOR_TAKEOFF
            rospy.loginfo("[master TaskFSM] TASK_IDLE -> TASK_READY_FOR_TAKEOFF")

    # ============================================================
    # 来自 sync_gate_node 的事件
    # ============================================================

    def _event_cb(self, msg: String):
        """
        处理同步器发来的本机事件。

        TaskFSM 不处理 PREPARE / COMMIT / ACK。
        TaskFSM 只处理任务层事件：
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
                "[%s TaskFSM] request accepted action=%s sync_id=%s",
                self.self_id,
                action,
                data.get("sync_id", ""),
            )

        elif event == EVENT_REQUEST_REJECTED:
            self.request_in_flight = False
            rospy.logwarn(
                "[%s TaskFSM] request rejected action=%s reason=%s",
                self.self_id,
                action,
                data.get("reason", ""),
            )

        elif event == EVENT_SCHEDULED:
            # SCHEDULED 表示：
            # COMMIT 已完成，t0 已经确定。
            # TaskFSM 保存该事件，并自己用 now >= t0 判断启动。
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

        elif event == EVENT_START:
            # START 是兜底事件。
            # 如果 TaskFSM 已经根据 SCHEDULED 自己启动，则 expected_action 会改变，
            # 这里就不会重复启动。
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

            self.task_state = self.TASK_ABORTED
            self.request_in_flight = False
            self.scheduled_event = None

    # ============================================================
    # 主循环
    # ============================================================

    def spin(self):
        rate = rospy.Rate(self.control_rate_hz)

        while not rospy.is_shutdown():
            if self.role == "master" and self.auto_start:
                self.start()

            self._publish_status_periodically()

            # 推荐方式：
            # 收到 SCHEDULED 后，任务层自己看 now >= t0 后启动。
            # 这样比等待 START 消息更准。
            self._start_scheduled_action_if_due()

            self._tick_task_50hz()

            rate.sleep()

    def _tick_task_50hz(self):
        """
        任务状态机主逻辑。

        注意：
            这里的状态全部是任务状态，不是同步状态。
        """
        if self.task_state == self.TASK_IDLE:
            return

        elif self.task_state == self.TASK_READY_FOR_TAKEOFF:
            self.safe_hold_50hz()
            self._master_request_sync_if_needed(
                action="takeoff",
                payload={
                    "height": self.takeoff_height,
                    "duration": self.takeoff_duration,
                },
            )

        elif self.task_state == self.TASK_TAKEOFF_RUNNING:
            tau = now_sec() - self.current_t0
            self.execute_takeoff_50hz(tau)

            if tau >= self.takeoff_duration:
                self._finish_action()
                self.task_state = self.TASK_READY_FOR_MISSION_SWITCH
                rospy.loginfo("[%s TaskFSM] -> TASK_READY_FOR_MISSION_SWITCH", self.self_id)

        elif self.task_state == self.TASK_READY_FOR_MISSION_SWITCH:
            self.safe_hold_50hz()
            self._master_request_sync_if_needed(
                action="switch_mission",
                payload={"mission_name": "cooperative_route_01"},
            )

        elif self.task_state == self.TASK_MISSION_SWITCHING:
            tau = now_sec() - self.action_start_wall_time
            self.execute_switch_mission_50hz(tau)

            if tau >= self.switch_mission_duration:
                self._finish_action()
                self.task_state = self.TASK_READY_FOR_TRANSPORT_CONTROL
                rospy.loginfo("[%s TaskFSM] -> TASK_READY_FOR_TRANSPORT_CONTROL", self.self_id)

        elif self.task_state == self.TASK_READY_FOR_TRANSPORT_CONTROL:
            self.safe_hold_50hz()
            self._master_request_sync_if_needed(
                action="start_transport_control",
                payload={"controller": "sling_load_controller_v1"},
            )

        elif self.task_state == self.TASK_TRANSPORT_CONTROL_RUNNING:
            tau = now_sec() - self.current_t0
            self.execute_transport_control_50hz(tau)

            if tau >= self.transport_duration:
                self._finish_action()
                self.task_state = self.TASK_READY_FOR_RELEASE
                rospy.loginfo("[%s TaskFSM] -> TASK_READY_FOR_RELEASE", self.self_id)

        elif self.task_state == self.TASK_READY_FOR_RELEASE:
            self.safe_hold_50hz()
            self._master_request_sync_if_needed(
                action="release_load",
                payload={"mode": "soft"},
            )

        elif self.task_state == self.TASK_RELEASE_RUNNING:
            tau = now_sec() - self.current_t0
            self.execute_release_50hz(tau)

            if tau >= self.release_duration:
                self._finish_action()
                self.task_state = self.TASK_FINISHED
                rospy.loginfo("[%s TaskFSM] -> TASK_FINISHED", self.self_id)

        elif self.task_state == self.TASK_FINISHED:
            self.safe_hold_50hz()

        elif self.task_state == self.TASK_ABORTED:
            self.safe_abort_50hz()

    # ============================================================
    # TaskFSM -> SyncGate：状态发布
    # ============================================================

    def _publish_status_periodically(self):
        """
        周期性发布任务状态给本机 sync_gate_node。

        sync_gate_node 用这个状态判断：
            本机是否可以参与某个 action 的同步。
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

    # ============================================================
    # master TaskFSM -> master SyncGate：同步申请
    # ============================================================

    def _master_request_sync_if_needed(self, action: str, payload: Dict[str, Any]):
        """
        只有 master 任务状态机会主动发布同步申请。
        slave 不会主动 request。
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
    # SCHEDULED / START 触发任务动作
    # ============================================================

    def _start_scheduled_action_if_due(self):
        """
        收到 SCHEDULED 后，TaskFSM 自己根据 t0 启动任务。

        这样避免等待 START 事件造成的额外 ROS 调度延迟。
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
        根据同步器的 SCHEDULED 或 START 事件，进入任务执行状态。
        """
        action = event.get("action", "")
        expected = self.expected_action()

        if action != expected:
            rospy.logwarn(
                "[%s TaskFSM] ignore event action=%s expected=%s",
                self.self_id,
                action,
                expected,
            )
            return

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
            "[%s TaskFSM] START action=%s sync_id=%s t0=%.3f",
            self.self_id,
            action,
            self.current_sync_id,
            self.current_t0,
        )

        if action == "takeoff":
            self.takeoff_height = float(
                self.current_payload.get("height", self.takeoff_height)
            )
            self.takeoff_duration = float(
                self.current_payload.get("duration", self.takeoff_duration)
            )
            self.task_state = self.TASK_TAKEOFF_RUNNING

        elif action == "switch_mission":
            self.task_state = self.TASK_MISSION_SWITCHING

        elif action == "start_transport_control":
            self.task_state = self.TASK_TRANSPORT_CONTROL_RUNNING

        elif action == "release_load":
            self.task_state = self.TASK_RELEASE_RUNNING

        else:
            rospy.logerr("[%s TaskFSM] unknown action=%s", self.self_id, action)
            self.task_state = self.TASK_ABORTED

    def _finish_action(self):
        """清理当前动作信息。"""
        self.current_action = ""
        self.current_payload = {}
        self.current_sync_id = ""
        self.current_t0 = 0.0
        self.action_start_wall_time = 0.0
        self.scheduled_event = None
        self.request_in_flight = False

    def expected_action(self) -> str:
        """
        当前任务状态下，下一次期望同步的 action。
        """
        if self.task_state == self.TASK_READY_FOR_TAKEOFF:
            return "takeoff"

        if self.task_state == self.TASK_READY_FOR_MISSION_SWITCH:
            return "switch_mission"

        if self.task_state == self.TASK_READY_FOR_TRANSPORT_CONTROL:
            return "start_transport_control"

        if self.task_state == self.TASK_READY_FOR_RELEASE:
            return "release_load"

        return ""

    # ============================================================
    # 具体任务动作
    # ============================================================

    def safe_hold_50hz(self):
        """
        等待同步、任务完成、或者空闲保持时调用。

        真实 PX4/MAVROS 中可以在这里持续发布 hold setpoint。
        """
        pass

    def safe_abort_50hz(self):
        """
        异常状态下调用。

        真实飞行中可以：
            - 切 HOLD
            - 发布当前位置 setpoint
            - 触发 LAND
        """
        pass

    def execute_takeoff_50hz(self, tau: float):
        """
        起飞动作。

        tau = now - t0。
        多机同步的核心就是所有飞机基于同一个 t0 计算 tau。
        """
        T = max(self.takeoff_duration, 1e-3)
        h = self.takeoff_height

        s = min(max(tau / T, 0.0), 1.0)
        sigma = 10 * s**3 - 15 * s**4 + 6 * s**5
        z_des = h * sigma

        rospy.loginfo_throttle(
            1.0,
            "[%s TaskFSM] takeoff tau=%.2f z_des=%.2f",
            self.self_id,
            tau,
            z_des,
        )

    def execute_switch_mission_50hz(self, tau: float):
        rospy.loginfo_throttle(
            1.0,
            "[%s TaskFSM] switch_mission tau=%.2f",
            self.self_id,
            tau,
        )

    def execute_transport_control_50hz(self, tau: float):
        rospy.loginfo_throttle(
            1.0,
            "[%s TaskFSM] transport_control tau=%.2f",
            self.self_id,
            tau,
        )

    def execute_release_50hz(self, tau: float):
        rospy.loginfo_throttle(
            1.0,
            "[%s TaskFSM] release_load tau=%.2f",
            self.self_id,
            tau,
        )


def main():
    rospy.init_node("task_fsm_node")
    node = TaskFSMNode()
    node.spin()


if __name__ == "__main__":
    main()