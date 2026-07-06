#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
minimal_sync_test_node.py

最小同步验证节点。

作用：
    1. 模拟 task_fsm_node，和 sync_gate_node 通信；
    2. 周期性发布本机 ready 状态；
    3. master 收到 /<self_id>/sync_test/start 后，向 sync_gate_node 发起同步请求；
    4. master/slave 收到 SCHEDULED 后保存 t0；
    5. 到 t0 后本机进入 RUNNING，并打印 now - t0；
    6. 运行 test_duration 秒后回到 READY，可重复测试。

验证链路：
    minimal_sync_test_node -> sync_gate_node:
        /<self_id>/task/sync_status
        /<self_id>/task/sync_request

    sync_gate_node -> minimal_sync_test_node:
        /<self_id>/task/sync_event

    上面三个接口均使用 aofe_star/JsonPayload.srv，payload 是 JSON 字符串。
"""

import json
from typing import Optional

import rospy
from std_msgs.msg import String, Bool
from aofe_star.srv import JsonPayload, JsonPayloadResponse


EVENT_REQUEST_ACCEPTED = "REQUEST_ACCEPTED"
EVENT_REQUEST_REJECTED = "REQUEST_REJECTED"
EVENT_SCHEDULED = "SCHEDULED"
EVENT_START = "START"
EVENT_ABORT = "ABORT"


STATE_READY = "READY"
STATE_WAITING_SYNC = "WAITING_SYNC"
STATE_SCHEDULED = "SCHEDULED"
STATE_RUNNING = "RUNNING"
STATE_DONE = "DONE"
STATE_ABORTED = "ABORTED"


def now_sec() -> float:
    return rospy.Time.now().to_sec()


def safe_json_loads(text: str) -> Optional[dict]:
    try:
        return json.loads(text)
    except Exception:
        return None


class MinimalSyncTestNode:
    def __init__(self):
        self.role = rospy.get_param("~role", "slave").lower().strip()
        self.self_id = rospy.get_param("~self_id", "uav4")

        if self.role not in ["master", "slave"]:
            raise RuntimeError("~role must be master or slave")

        self.action_name = rospy.get_param("~action_name", "test_sync")
        self.test_duration = float(rospy.get_param("~test_duration", 3.0))
        self.status_rate = float(rospy.get_param("~test_status_rate", 10.0))
        self.request_rate = float(rospy.get_param("~test_request_rate", 1.0))
        self.loop_rate = float(rospy.get_param("~loop_rate", 50.0))
        self.sync_service_timeout = float(rospy.get_param("~sync_service_timeout", 0.2))
        self.auto_reset = bool(rospy.get_param("~auto_reset", True))

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
            f"/{self.self_id}/sync_test/start",
        )

        self.result_topic = rospy.get_param(
            "~result_topic",
            f"/{self.self_id}/sync_test/result",
        )

        self.state = STATE_READY

        self.request_in_flight = False
        self.request_counter = 0
        self.last_request_pub_time = 0.0
        self.last_status_pub_time = 0.0

        self.scheduled_event = None
        self.current_sync_id = ""
        self.current_t0 = 0.0
        self.running_start_time = 0.0

        self.cmd_start = False

        self.status_srv = rospy.ServiceProxy(self.task_status_topic, JsonPayload)
        self.request_srv = rospy.ServiceProxy(self.task_request_topic, JsonPayload)

        self.result_pub = rospy.Publisher(
            self.result_topic,
            String,
            queue_size=20,
        )

        self.task_event_srv = rospy.Service(
            self.task_event_topic,
            JsonPayload,
            self._event_srv,
        )

        rospy.Subscriber(
            self.start_topic,
            Bool,
            self._start_cb,
            queue_size=5,
        )

        rospy.loginfo(
            "[MinimalSyncTest] role=%s self_id=%s action=%s state=%s",
            self.role,
            self.self_id,
            self.action_name,
            self.state,
        )

    def _start_cb(self, msg: Bool):
        """
        只有 master 需要发 start。
        slave 不主动发起同步，只保持 READY 等待 master。
        """
        if msg.data:
            self.cmd_start = True
            rospy.loginfo("[%s MinimalSyncTest] received start command", self.self_id)

    def _event_srv(self, req):
        data = safe_json_loads(req.payload)
        if not data:
            return JsonPayloadResponse(False, "invalid_json", "")

        self._handle_event(data)
        return JsonPayloadResponse(True, "ok", "")

    def _handle_event(self, data: dict):
        event = data.get("event", "")
        action = data.get("action", "")

        if action and action != self.action_name:
            rospy.logwarn(
                "[%s MinimalSyncTest] ignore event action=%s, expected=%s",
                self.self_id,
                action,
                self.action_name,
            )
            return

        if event == EVENT_REQUEST_ACCEPTED:
            self.request_in_flight = True
            self.state = STATE_WAITING_SYNC

            rospy.loginfo(
                "[%s MinimalSyncTest] REQUEST_ACCEPTED sync_id=%s",
                self.self_id,
                data.get("sync_id", ""),
            )

        elif event == EVENT_REQUEST_REJECTED:
            self.request_in_flight = False
            self.state = STATE_READY

            rospy.logwarn(
                "[%s MinimalSyncTest] REQUEST_REJECTED reason=%s",
                self.self_id,
                data.get("reason", ""),
            )

        elif event == EVENT_SCHEDULED:
            self.scheduled_event = data
            self.current_sync_id = data.get("sync_id", "")
            self.current_t0 = float(data.get("t0", 0.0))
            self.state = STATE_SCHEDULED

            rospy.loginfo(
                "[%s MinimalSyncTest] SCHEDULED sync_id=%s t0=%.6f now=%.6f dt=%.3f",
                self.self_id,
                self.current_sync_id,
                self.current_t0,
                now_sec(),
                self.current_t0 - now_sec(),
            )

        elif event == EVENT_START:
            # START 是兜底；正常情况下节点会根据 SCHEDULED 的 t0 自己启动。
            if self.state in [STATE_READY, STATE_WAITING_SYNC, STATE_SCHEDULED]:
                rospy.loginfo(
                    "[%s MinimalSyncTest] START fallback received sync_id=%s",
                    self.self_id,
                    data.get("sync_id", ""),
                )
                self._start_running(data)

        elif event == EVENT_ABORT:
            self.state = STATE_ABORTED
            self.request_in_flight = False
            self.scheduled_event = None

            rospy.logerr(
                "[%s MinimalSyncTest] ABORT reason=%s",
                self.self_id,
                data.get("reason", ""),
            )

    def spin(self):
        rate = rospy.Rate(self.loop_rate)

        while not rospy.is_shutdown():
            self._publish_status_periodically()

            if self.role == "master" and self.cmd_start:
                self._master_request_sync_if_needed()

            self._start_if_scheduled_due()
            self._check_running_done()

            rate.sleep()

    def _call_json_service(
        self,
        service_proxy,
        service_name: str,
        data: dict,
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
                "[%s MinimalSyncTest] %s service call failed name=%s err=%s",
                self.self_id,
                label,
                service_name,
                str(e),
            )
            return None

        if handle_response_event and resp.payload:
            event_data = safe_json_loads(resp.payload)
            if event_data:
                self._handle_event(event_data)
            else:
                rospy.logwarn(
                    "[%s MinimalSyncTest] %s service returned invalid event json: %s",
                    self.self_id,
                    label,
                    resp.payload,
                )

        if not resp.ok and not (handle_response_event and resp.payload):
            rospy.logwarn_throttle(
                2.0,
                "[%s MinimalSyncTest] %s service returned not ok name=%s reason=%s",
                self.self_id,
                label,
                service_name,
                resp.reason,
            )

        return resp

    def _publish_status_periodically(self):
        t = now_sec()
        period = 1.0 / max(self.status_rate, 1e-6)

        if t - self.last_status_pub_time < period:
            return

        self.last_status_pub_time = t

        ready = self.state == STATE_READY
        expected_action = self.action_name if ready else ""

        data = {
            "src": self.self_id,
            "stamp": t,
            "task_state": self.state,
            "ready": ready,
            "expected_action": expected_action,
            "reason": "ready_for_" + self.action_name if ready else self.state,
            "current_action": self.action_name if self.state in [STATE_WAITING_SYNC, STATE_SCHEDULED, STATE_RUNNING] else "",
            "current_sync_id": self.current_sync_id,
        }

        self._call_json_service(
            self.status_srv,
            self.task_status_topic,
            data,
            "sync_status",
        )

    def _master_request_sync_if_needed(self):
        if self.state != STATE_READY:
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

        request_id = f"{self.self_id}_{self.request_counter:04d}_{self.action_name}"

        data = {
            "src": self.self_id,
            "stamp": t,
            "request_id": request_id,
            "action": self.action_name,
            "payload": {
                "test_duration": self.test_duration,
                "request_id": request_id,
                "note": "minimal sync test",
            },
        }

        self._call_json_service(
            self.request_srv,
            self.task_request_topic,
            data,
            "sync_request",
            handle_response_event=True,
        )

        rospy.loginfo(
            "[%s MinimalSyncTest] call sync_request action=%s request_id=%s",
            self.self_id,
            self.action_name,
            request_id,
        )

    def _start_if_scheduled_due(self):
        if self.scheduled_event is None:
            return

        if self.current_t0 <= 0:
            return

        now = now_sec()
        remain = self.current_t0 - now

        # 还很早，不启动
        if remain > 0.02:
            return

        # 距离 t0 进入 20ms 内，做一次更精细等待
        # 注意：这里会短暂阻塞当前节点，所以只适合临近触发时使用
        while not rospy.is_shutdown():
            now = now_sec()
            remain = self.current_t0 - now

            if remain <= 0:
                break

            # 还剩 2ms 以上，用短 sleep，避免 CPU 占用太高
            if remain > 0.002:
                rospy.sleep(0.0005)
            else:
                # 最后 2ms busy wait，提高触发精度
                pass

        self._start_running(self.scheduled_event)

    def _start_running(self, event: dict):
        if self.state == STATE_RUNNING:
            return

        self.scheduled_event = None
        self.request_in_flight = False
        self.cmd_start = False

        self.current_sync_id = event.get("sync_id", self.current_sync_id)
        self.current_t0 = float(event.get("t0", self.current_t0))
        self.running_start_time = now_sec()

        start_error = self.running_start_time - self.current_t0

        self.state = STATE_RUNNING

        rospy.loginfo(
            "[%s MinimalSyncTest] RUNNING sync_id=%s now=%.6f t0=%.6f error=%.6f sec",
            self.self_id,
            self.current_sync_id,
            self.running_start_time,
            self.current_t0,
            start_error,
        )

        self._publish_result(
            event="RUNNING",
            extra={
                "start_error": start_error,
            },
        )

    def _check_running_done(self):
        if self.state != STATE_RUNNING:
            return

        elapsed = now_sec() - self.running_start_time

        if elapsed >= self.test_duration:
            self.state = STATE_DONE

            rospy.loginfo(
                "[%s MinimalSyncTest] DONE sync_id=%s elapsed=%.2f",
                self.self_id,
                self.current_sync_id,
                elapsed,
            )

            self._publish_result(
                event="DONE",
                extra={
                    "elapsed": elapsed,
                },
            )

            if self.auto_reset:
                self._reset_to_ready()

    def _reset_to_ready(self):
        self.request_in_flight = False
        self.scheduled_event = None
        self.current_sync_id = ""
        self.current_t0 = 0.0
        self.running_start_time = 0.0
        self.cmd_start = False
        self.state = STATE_READY

        rospy.loginfo("[%s MinimalSyncTest] reset to READY", self.self_id)

    def _publish_result(self, event: str, extra: Optional[dict] = None):
        data = {
            "src": self.self_id,
            "stamp": now_sec(),
            "role": self.role,
            "state": self.state,
            "event": event,
            "action": self.action_name,
            "sync_id": self.current_sync_id,
            "t0": self.current_t0,
            "extra": extra if isinstance(extra, dict) else {},
        }

        self.result_pub.publish(
            String(data=json.dumps(data, separators=(",", ":")))
        )


def main():
    rospy.init_node("minimal_sync_test_node")
    node = MinimalSyncTestNode()
    node.spin()


if __name__ == "__main__":
    main()
