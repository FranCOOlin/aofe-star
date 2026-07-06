#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
sync_gate_node.py

功能：
    通用多机同步器节点。

核心设计：
    1. sync_gate_node 只负责同步协议，不负责具体任务。
    2. task_fsm_node 只负责任务状态机，不负责 PREPARE / COMMIT / ACK。
    3. 两个节点之间只通过本机 service 通信。
    4. 多机之间只由各自的 sync_gate_node 通信。

本机 service：
    task_fsm_node -> sync_gate_node:
        /<self_id>/task/sync_status
        /<self_id>/task/sync_request

    sync_gate_node -> task_fsm_node:
        /<self_id>/task/sync_event

    上面三个接口均使用 aofe_star/JsonPayload.srv，payload 是 JSON 字符串。

多机 topic：
    /sync/command
    /sync/ack
    /sync/status

同步协议：
    master task_fsm_node 发布 sync_request(action)
        ↓
    master sync_gate_node 创建 SyncSession，但 t0 = 0.0
        ↓
    master 发布 PREPARE(action, payload, t0=0.0)
        ↓
    slave 收到 PREPARE，只检查自己是否 ready，不检查 t0
        ↓
    slave 返回 ACK_PREPARE
        ↓
    master 收齐 ACK_PREPARE 后，生成最终 t0 = now + start_delay
        ↓
    master 发布 COMMIT(action, payload, t0=final_t0)
        ↓
    slave 收到 COMMIT，检查 t0 是否有效，并写入 current_sync.t0
        ↓
    slave 返回 ACK_COMMIT，并向本机 task_fsm_node 发布 SCHEDULED
        ↓
    master 收齐 ACK_COMMIT 后，向本机 task_fsm_node 发布 SCHEDULED
        ↓
    到达 t0 后，master/slave 都向本机 task_fsm_node 发布 START
"""

import json
import re
import time
import uuid
import threading
from typing import Any, Dict, List, Optional, Tuple

import rospy
from std_msgs.msg import String
from aofe_star.srv import JsonPayload, JsonPayloadResponse


# ============================================================
# 多机同步协议消息类型
# ============================================================

MSG_CMD = "CMD"          # master sync_gate -> slave sync_gate
MSG_ACK = "ACK"          # slave sync_gate -> master sync_gate
MSG_STATUS = "STATUS"    # slave sync_gate -> master sync_gate

CMD_PREPARE = "PREPARE"
CMD_COMMIT = "COMMIT"
CMD_ABORT = "ABORT"

ACK_PREPARE = "ACK_PREPARE"
ACK_COMMIT = "ACK_COMMIT"


# ============================================================
# 本机 sync_gate_node -> task_fsm_node 的事件类型
# ============================================================

EVENT_REQUEST_ACCEPTED = "REQUEST_ACCEPTED"
EVENT_REQUEST_REJECTED = "REQUEST_REJECTED"

# 同步已提交，t0 已确定。
# task_fsm_node 收到后可以保存 t0 并提前准备轨迹。
EVENT_SCHEDULED = "SCHEDULED"

# 当前时间已到 t0。
# task_fsm_node 收到后可以兜底启动。
# 但更推荐 task_fsm_node 收到 SCHEDULED 后自己用 now >= t0 启动。
EVENT_START = "START"

# 同步失败或被取消。
EVENT_ABORT = "ABORT"


# ============================================================
# sync_gate_node 内部同步状态
# 这些状态只属于同步器，不属于任务状态机。
# ============================================================

SYNC_IDLE = "SYNC_IDLE"

# master：正在发 PREPARE，等待 ACK_PREPARE。
SYNC_PREPARE = "SYNC_PREPARE"

# master：正在发 COMMIT，等待 ACK_COMMIT。
SYNC_COMMIT = "SYNC_COMMIT"

# slave：已经接受 PREPARE，正在等待 COMMIT。
SYNC_WAIT_COMMIT = "SYNC_WAIT_COMMIT"

# master/slave：COMMIT 已确认，等待 t0。
SYNC_WAIT_T0 = "SYNC_WAIT_T0"

# master：正在重复广播 ABORT。
SYNC_ABORTING = "SYNC_ABORTING"


# ============================================================
# 工具函数
# ============================================================

def now_sec() -> float:
    """返回 ROS 时间，单位秒。"""
    return rospy.Time.now().to_sec()


def safe_json_loads(text: str) -> Optional[dict]:
    """安全解析 JSON 字符串。"""
    try:
        return json.loads(text)
    except Exception:
        return None


def sanitize_id(text: str) -> str:
    """
    把字符串清理成适合做 ID 的形式。

    例如：
        "coop lift test 001" -> "coop_lift_test_001"
        "start transport control" -> "start_transport_control"
    """
    text = str(text).strip()
    text = re.sub(r"[^a-zA-Z0-9_\-]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_")


def parse_list(value: Any) -> List[str]:
    """
    解析 participants。

    支持：
        "uav1,uav2"
    或：
        ["uav1", "uav2"]
    """
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, str):
        return [x.strip() for x in value.split(",") if x.strip()]
    return []


def make_default_run_id(run_label: str) -> str:
    """
    自动生成 run_id。

    run_id 表示本次实验流程编号，不表示某一次同步动作。

    推荐正式实验中在 launch 里显式指定：
        <param name="run_id" value="coop_lift_test_001"/>
    """
    label = sanitize_id(run_label) or "sync_run"
    stamp = time.strftime("%Y%m%d_%H%M%S")
    short_uid = uuid.uuid4().hex[:6]
    return f"{label}_{stamp}_{short_uid}"


# ============================================================
# 一次同步会话
# ============================================================

class SyncSession:
    """
    一次同步会话。

    例子：
        run_id       = "coop_lift_test_001"
        sync_seq     = 1
        action       = "takeoff"
        sync_id      = "coop_lift_test_001_0001_takeoff"
        payload      = {"height": 3.0, "duration": 6.0}
        t0           = 0.0   # PREPARE 阶段
        t0           = 123.0 # COMMIT 阶段写入最终 t0
        participants = ["uav1", "uav2"]
    """

    def __init__(
        self,
        run_id: str,
        sync_seq: int,
        action: str,
        payload: Optional[Dict[str, Any]],
        t0: float,
        participants: Optional[List[str]] = None,
        sync_id: str = "",
    ):
        self.run_id = sanitize_id(run_id)
        self.sync_seq = int(sync_seq)
        self.action = sanitize_id(action)
        self.payload = payload if isinstance(payload, dict) else {}
        self.t0 = float(t0)
        self.participants = participants if participants is not None else []

        if sync_id:
            self.sync_id = sanitize_id(sync_id)
        else:
            self.sync_id = f"{self.run_id}_{self.sync_seq:04d}_{self.action}"

        # master 侧记录 ACK。
        self.prepare_acks: Dict[str, dict] = {}
        self.commit_acks: Dict[str, dict] = {}

        # master 侧记录拒绝原因。
        self.prepare_rejects: Dict[str, str] = {}
        self.commit_rejects: Dict[str, str] = {}

    def to_dict(self) -> dict:
        """转换为 dict，用于 JSON 发布。"""
        return {
            "run_id": self.run_id,
            "sync_seq": self.sync_seq,
            "sync_id": self.sync_id,
            "action": self.action,
            "payload": self.payload,
            "t0": self.t0,
            "participants": self.participants,
        }

    @staticmethod
    def from_msg(data: dict) -> Optional["SyncSession"]:
        """
        从 JSON 消息恢复 SyncSession。

        注意：
            PREPARE 阶段 t0 可以等于 0.0。
            因此这里允许 t0 == 0.0。
            只有 t0 < 0 才非法。
        """
        try:
            run_id = str(data.get("run_id", ""))
            sync_seq = int(data.get("sync_seq", 0))
            sync_id = str(data.get("sync_id", ""))
            action = str(data.get("action", ""))
            payload = data.get("payload", {})
            t0 = float(data.get("t0", 0.0))
            participants = parse_list(data.get("participants", []))

            if not run_id:
                return None
            if sync_seq <= 0:
                return None
            if not sync_id:
                return None
            if not action:
                return None
            if t0 < 0:
                return None

            return SyncSession(
                run_id=run_id,
                sync_seq=sync_seq,
                action=action,
                payload=payload,
                t0=t0,
                participants=participants,
                sync_id=sync_id,
            )

        except Exception:
            return None


# ============================================================
# SyncGateNode
# ============================================================

class SyncGateNode:
    def __init__(self):
        # -----------------------------
        # 基本身份参数
        # -----------------------------
        self.role = rospy.get_param("~role", "slave").lower().strip()
        self.self_id = rospy.get_param("~self_id", "uav1")

        if self.role not in ["master", "slave"]:
            raise RuntimeError("~role must be master or slave")

        # participants 是 master 需要等待 ACK 的从机列表。
        # 一般不包含 master 自己。
        self.default_participants = parse_list(
            rospy.get_param("~participants", "uav1,uav2")
        )

        # -----------------------------
        # run_id
        # -----------------------------
        run_id = rospy.get_param("~run_id", "")
        run_label = rospy.get_param("~run_label", "coop_sync")

        if run_id:
            self.run_id = sanitize_id(run_id)
        else:
            self.run_id = make_default_run_id(run_label)

        self.sync_seq = 0

        # -----------------------------
        # 多机同步器之间的全局 topic
        # -----------------------------
        self.command_topic = rospy.get_param("~command_topic", "/sync/command")
        self.ack_topic = rospy.get_param("~ack_topic", "/sync/ack")
        self.global_status_topic = rospy.get_param("~global_status_topic", "/sync/status")

        # -----------------------------
        # 本机 task_fsm_node 与 sync_gate_node 之间的 service
        # 参数名保留 *_topic，是为了兼容已有 launch 文件。
        # -----------------------------
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

        self.protocol = rospy.get_param("~protocol", "sync_gate_v4")

        # -----------------------------
        # 频率参数
        # -----------------------------
        self.control_rate_hz = float(rospy.get_param("~control_rate_hz", 50.0))

        # 同步协议消息不是飞控 setpoint，不需要 50Hz。
        self.command_rate = float(rospy.get_param("~command_rate", 5.0))
        self.ack_rate = float(rospy.get_param("~ack_rate", 5.0))
        self.global_status_rate = float(rospy.get_param("~global_status_rate", 1.0))

        # -----------------------------
        # 时间与超时参数
        # -----------------------------
        self.start_delay = float(rospy.get_param("~start_delay", 5.0))
        self.min_t0_margin = float(rospy.get_param("~min_t0_margin", 1.0))

        self.task_status_timeout = float(rospy.get_param("~task_status_timeout", 1.0))
        self.global_status_timeout = float(rospy.get_param("~global_status_timeout", 2.0))

        self.prepare_timeout = float(rospy.get_param("~prepare_timeout", 3.0))
        self.commit_timeout = float(rospy.get_param("~commit_timeout", 3.0))
        self.abort_duration = float(rospy.get_param("~abort_duration", 2.0))

        self.require_ready_status = bool(rospy.get_param("~require_ready_status", True))
        self.task_event_service_timeout = float(
            rospy.get_param("~task_event_service_timeout", 0.2)
        )

        # -----------------------------
        # 同步器内部状态
        # -----------------------------
        self.sync_state = SYNC_IDLE
        self.sync_state_start_time = now_sec()

        self.current_sync: Optional[SyncSession] = None

        # 本机任务状态，由本机 task_fsm_node 发布。
        self.local_task_status: Optional[dict] = None
        self.local_task_status_time = 0.0

        # master 侧保存所有 slave 的全局状态。
        self.remote_sync_status: Dict[str, dict] = {}

        # 用于过滤旧消息。
        self.closed_sync_ids = set()
        self.closed_sync_order: List[str] = []
        self.closed_cache_size = int(rospy.get_param("~closed_cache_size", 100))
        self.last_seq_by_run: Dict[str, int] = {}

        self.abort_reason = ""

        self.last_command_pub_time = 0.0
        self.last_ack_pub_time = 0.0
        self.last_global_status_pub_time = 0.0

        self.lock = threading.RLock()

        # -----------------------------
        # ROS pub/sub/service
        # -----------------------------
        self.command_pub = rospy.Publisher(self.command_topic, String, queue_size=50)
        self.ack_pub = rospy.Publisher(self.ack_topic, String, queue_size=100)
        self.global_status_pub = rospy.Publisher(
            self.global_status_topic,
            String,
            queue_size=100,
        )
        self.task_event_srv = rospy.ServiceProxy(self.task_event_topic, JsonPayload)

        rospy.Subscriber(self.command_topic, String, self._command_cb, queue_size=100)
        rospy.Subscriber(self.ack_topic, String, self._ack_cb, queue_size=100)
        rospy.Subscriber(
            self.global_status_topic,
            String,
            self._global_status_cb,
            queue_size=100,
        )

        self.task_status_srv = rospy.Service(
            self.task_status_topic,
            JsonPayload,
            self._task_status_srv,
        )
        self.task_request_srv = rospy.Service(
            self.task_request_topic,
            JsonPayload,
            self._task_request_srv,
        )

        rospy.loginfo(
            "[SyncGateNode] role=%s self_id=%s run_id=%s participants=%s",
            self.role,
            self.self_id,
            self.run_id,
            self.default_participants,
        )

    # ============================================================
    # 本机 task_fsm_node -> sync_gate_node
    # ============================================================

    def _task_status_srv(self, req):
        """
        接收本机 task_fsm_node 上报的任务状态。

        这个状态用于判断：
            本机是否可以参与某个 action 的同步。
        """
        data = safe_json_loads(req.payload)
        if not data:
            return JsonPayloadResponse(False, "invalid_json", "")

        if data.get("src", "") != self.self_id:
            return JsonPayloadResponse(False, "src_mismatch", "")

        with self.lock:
            self.local_task_status = data
            self.local_task_status_time = now_sec()

        return JsonPayloadResponse(True, "ok", "")

    def _task_request_srv(self, req):
        """
        接收本机 task_fsm_node 的同步申请。

        只有 master 同步器会接受 request。
        slave 同步器如果收到 request，会直接拒绝。
        """
        data = safe_json_loads(req.payload)
        if not data:
            return JsonPayloadResponse(False, "invalid_json", "")

        if data.get("src", "") != self.self_id:
            return JsonPayloadResponse(False, "src_mismatch", "")

        action = sanitize_id(data.get("action", ""))
        payload = data.get("payload", {})
        request_id = data.get("request_id", "")

        participants = parse_list(data.get("participants", [])) or self.default_participants

        if not action:
            event_data = self._make_task_event(
                event_type=EVENT_REQUEST_REJECTED,
                action="",
                payload={},
                sync_id="",
                t0=0.0,
                reason="empty_action",
                extra={"request_id": request_id},
            )
            return self._json_response(False, "empty_action", event_data)

        if self.role != "master":
            event_data = self._make_task_event(
                event_type=EVENT_REQUEST_REJECTED,
                action=action,
                payload=payload,
                sync_id="",
                t0=0.0,
                reason="only_master_can_request_sync",
                extra={"request_id": request_id},
            )
            return self._json_response(False, "only_master_can_request_sync", event_data)

        with self.lock:
            ok, reason, event_data = self._start_new_sync(action, payload, participants)

        if not ok:
            event_data = self._make_task_event(
                event_type=EVENT_REQUEST_REJECTED,
                action=action,
                payload=payload,
                sync_id="",
                t0=0.0,
                reason=reason,
                extra={"request_id": request_id},
            )
            return self._json_response(False, reason, event_data)

        return self._json_response(True, reason, event_data)

    def _json_response(self, ok: bool, reason: str, payload: Optional[Dict[str, Any]] = None):
        payload_text = ""
        if isinstance(payload, dict):
            payload_text = json.dumps(payload, separators=(",", ":"))

        return JsonPayloadResponse(
            ok=bool(ok),
            reason=reason,
            payload=payload_text,
        )

    def _make_task_event(
        self,
        event_type: str,
        action: str,
        payload: Optional[Dict[str, Any]],
        sync_id: str,
        t0: float,
        reason: str = "",
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        data = {
            "event": event_type,
            "src": self.self_id,
            "stamp": now_sec(),
            "action": action,
            "payload": payload if isinstance(payload, dict) else {},
            "sync_id": sync_id,
            "t0": float(t0),
            "reason": reason,
            "extra": extra if isinstance(extra, dict) else {},
            "run_id": self.run_id,
            "sync_seq": 0,
        }

        if self.current_sync is not None:
            data["run_id"] = self.current_sync.run_id
            data["sync_seq"] = self.current_sync.sync_seq

        return data

    def _publish_task_event(
        self,
        event_type: str,
        action: str,
        payload: Optional[Dict[str, Any]],
        sync_id: str,
        t0: float,
        reason: str = "",
        extra: Optional[Dict[str, Any]] = None,
    ):
        """
        向本机 task_fsm_node 发布事件。

        这是同步器对任务层的唯一输出。
        """
        data = self._make_task_event(
            event_type=event_type,
            action=action,
            payload=payload,
            sync_id=sync_id,
            t0=t0,
            reason=reason,
            extra=extra,
        )

        payload_text = json.dumps(data, separators=(",", ":"))

        try:
            rospy.wait_for_service(
                self.task_event_topic,
                timeout=self.task_event_service_timeout,
            )
            resp = self.task_event_srv(payload_text)
        except (rospy.ROSException, rospy.ServiceException) as e:
            rospy.logwarn_throttle(
                2.0,
                "[SyncGate %s] task_event service call failed name=%s err=%s",
                self.self_id,
                self.task_event_topic,
                str(e),
            )
            return

        if not resp.ok:
            rospy.logwarn(
                "[SyncGate %s] task_event service returned not ok name=%s reason=%s",
                self.self_id,
                self.task_event_topic,
                resp.reason,
            )

    # ============================================================
    # master：开始一次新的同步
    # ============================================================

    def _start_new_sync(
        self,
        action: str,
        payload: Dict[str, Any],
        participants: List[str],
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        master 根据本机 TaskFSM 的 sync_request，创建新的同步会话。

        注意：
            这里不生成最终 t0。
            这里只创建 current_sync，并设置 t0 = 0.0。
            最终 t0 在收齐所有 ACK_PREPARE 后生成。
        """
        if self.current_sync is not None or self.sync_state != SYNC_IDLE:
            return False, f"sync_busy:{self.sync_state}", None

        ok, reason = self._check_local_task_ready(action)
        if not ok:
            return False, f"local_task_not_ready:{reason}", None

        ok, reason = self._check_all_remote_ready(action, participants)
        if not ok:
            return False, reason, None

        self.sync_seq += 1

        # 关键点：
        # PREPARE 阶段不生成最终 t0。
        # t0 = 0.0 表示尚未提交最终开始时间。
        self.current_sync = SyncSession(
            run_id=self.run_id,
            sync_seq=self.sync_seq,
            action=action,
            payload=payload,
            t0=0.0,
            participants=participants,
        )

        self.sync_state = SYNC_PREPARE
        self.sync_state_start_time = now_sec()
        self.last_command_pub_time = 0.0

        rospy.loginfo(
            "[SyncGate master] new sync PREPARE action=%s sync_id=%s participants=%s",
            action,
            self.current_sync.sync_id,
            participants,
        )

        # 通知 master 本机 TaskFSM：
        # 请求已被接受，但还没有最终 t0。
        event_data = self._make_task_event(
            event_type=EVENT_REQUEST_ACCEPTED,
            action=action,
            payload=payload,
            sync_id=self.current_sync.sync_id,
            t0=0.0,
            reason="prepare_started",
        )

        return True, "ok", event_data

    def _check_local_task_ready(self, action: str) -> Tuple[bool, str]:
        """
        检查 master 本机任务状态是否允许发起 action。
        """
        if self.local_task_status is None:
            return False, "no_local_task_status"

        age = now_sec() - self.local_task_status_time
        if age > self.task_status_timeout:
            return False, f"local_task_status_timeout:{age:.2f}"

        ready = bool(self.local_task_status.get("ready", False))
        expected_action = self.local_task_status.get("expected_action", "")

        if self.require_ready_status and not ready:
            return False, self.local_task_status.get("reason", "not_ready")

        if expected_action != action:
            return False, f"expected_action:{expected_action},request:{action}"

        return True, "ok"

    def _check_all_remote_ready(
        self,
        action: str,
        participants: List[str],
    ) -> Tuple[bool, str]:
        """
        master 在发起同步前，对从机做低频状态预检查。

        注意：
            这只是预检查。
            真正的确认仍然依赖 PREPARE/ACK_PREPARE。
        """
        t = now_sec()

        for uid in participants:
            if uid not in self.remote_sync_status:
                return False, f"{uid}_no_global_status"

            st = self.remote_sync_status[uid]
            age = t - float(st.get("stamp", 0.0))

            if age > self.global_status_timeout:
                return False, f"{uid}_global_status_timeout:{age:.2f}"

            ready = bool(st.get("ready", False))
            expected_action = st.get("expected_action", "")

            if self.require_ready_status and not ready:
                return False, f"{uid}_not_ready:{st.get('reason', '')}"

            if expected_action != action:
                return False, f"{uid}_expected_action:{expected_action},request:{action}"

        return True, "ok"

    # ============================================================
    # 多机同步器之间：回调
    # ============================================================

    def _global_status_cb(self, msg: String):
        """
        master 接收 slave 的全局状态。
        """
        if self.role != "master":
            return

        data = safe_json_loads(msg.data)
        if not data:
            return

        if data.get("protocol") != self.protocol:
            return
        if data.get("msg_type") != MSG_STATUS:
            return

        src = data.get("src", "")
        if src not in self.default_participants:
            return

        with self.lock:
            self.remote_sync_status[src] = data

    def _ack_cb(self, msg: String):
        """
        master 接收 slave 的 ACK。
        """
        if self.role != "master":
            return

        data = safe_json_loads(msg.data)
        if not data:
            return

        if data.get("protocol") != self.protocol:
            return
        if data.get("msg_type") != MSG_ACK:
            return

        with self.lock:
            if self.current_sync is None:
                return

            if data.get("sync_id", "") != self.current_sync.sync_id:
                return

            src = data.get("src", "")
            if src not in self.current_sync.participants:
                return

            ack_type = data.get("ack_type", "")
            accepted = bool(data.get("accepted", False))
            reason = data.get("reason", "")

            if ack_type == ACK_PREPARE:
                if accepted:
                    self.current_sync.prepare_acks[src] = data
                else:
                    self.current_sync.prepare_rejects[src] = reason or "prepare_rejected"

            elif ack_type == ACK_COMMIT:
                if accepted:
                    self.current_sync.commit_acks[src] = data
                else:
                    self.current_sync.commit_rejects[src] = reason or "commit_rejected"

    def _command_cb(self, msg: String):
        """
        slave 接收 master 的 PREPARE / COMMIT / ABORT。
        """
        if self.role != "slave":
            return

        data = safe_json_loads(msg.data)
        if not data:
            return

        if data.get("protocol") != self.protocol:
            return
        if data.get("msg_type") != MSG_CMD:
            return

        sync = SyncSession.from_msg(data)
        if sync is None:
            return

        if self.self_id not in sync.participants:
            return

        cmd = data.get("cmd", "")

        with self.lock:
            if cmd == CMD_PREPARE:
                self._slave_handle_prepare(sync)
            elif cmd == CMD_COMMIT:
                self._slave_handle_commit(sync)
            elif cmd == CMD_ABORT:
                self._slave_handle_abort(sync, data.get("reason", "master_abort"))

    # ============================================================
    # master 同步状态机
    # ============================================================

    def _tick_master(self):
        t = now_sec()

        if self.sync_state == SYNC_IDLE:
            return

        if self.current_sync is None:
            self._reset_sync_state()
            return

        if self.sync_state == SYNC_PREPARE:
            self._publish_command_periodically(CMD_PREPARE)

            if self.current_sync.prepare_rejects:
                self._master_abort(
                    f"prepare_rejected:{self.current_sync.prepare_rejects}"
                )
                return

            if self._all_prepare_acked():
                # 关键点：
                # 收齐所有 ACK_PREPARE 后，才生成最终 t0。
                final_t0 = now_sec() + self.start_delay
                self.current_sync.t0 = final_t0

                rospy.loginfo(
                    "[SyncGate master] all ACK_PREPARE, set final t0=%.3f sync_id=%s",
                    self.current_sync.t0,
                    self.current_sync.sync_id,
                )

                self.sync_state = SYNC_COMMIT
                self.sync_state_start_time = t
                self.last_command_pub_time = 0.0
                return

            if t - self.sync_state_start_time > self.prepare_timeout:
                self._master_abort("prepare_timeout")
                return

        elif self.sync_state == SYNC_COMMIT:
            self._publish_command_periodically(CMD_COMMIT)

            if self.current_sync.commit_rejects:
                self._master_abort(
                    f"commit_rejected:{self.current_sync.commit_rejects}"
                )
                return

            if self._all_commit_acked():
                rospy.loginfo(
                    "[SyncGate master] all ACK_COMMIT: %s",
                    self.current_sync.sync_id,
                )

                self.sync_state = SYNC_WAIT_T0
                self.sync_state_start_time = t

                # 同步已经正式提交，通知本机 TaskFSM。
                self._publish_task_event(
                    event_type=EVENT_SCHEDULED,
                    action=self.current_sync.action,
                    payload=self.current_sync.payload,
                    sync_id=self.current_sync.sync_id,
                    t0=self.current_sync.t0,
                    reason="scheduled",
                )
                return

            if t - self.sync_state_start_time > self.commit_timeout:
                self._master_abort("commit_timeout")
                return

            if self.current_sync.t0 - t < self.min_t0_margin:
                self._master_abort("t0_too_close_during_commit")
                return

        elif self.sync_state == SYNC_WAIT_T0:
            if t >= self.current_sync.t0:
                self._publish_task_event(
                    event_type=EVENT_START,
                    action=self.current_sync.action,
                    payload=self.current_sync.payload,
                    sync_id=self.current_sync.sync_id,
                    t0=self.current_sync.t0,
                    reason="start",
                )
                self._close_current_sync()
                self._reset_sync_state()

        elif self.sync_state == SYNC_ABORTING:
            self._publish_command_periodically(CMD_ABORT, reason=self.abort_reason)

            if t - self.sync_state_start_time > self.abort_duration:
                self._close_current_sync()
                self._reset_sync_state()

    def _all_prepare_acked(self) -> bool:
        return (
            self.current_sync is not None
            and all(uid in self.current_sync.prepare_acks for uid in self.current_sync.participants)
        )

    def _all_commit_acked(self) -> bool:
        return (
            self.current_sync is not None
            and all(uid in self.current_sync.commit_acks for uid in self.current_sync.participants)
        )

    def _publish_command_periodically(self, cmd: str, reason: str = ""):
        t = now_sec()
        period = 1.0 / max(self.command_rate, 1e-6)

        if t - self.last_command_pub_time < period:
            return

        self.last_command_pub_time = t
        self._publish_command(cmd, reason)

    def _publish_command(self, cmd: str, reason: str = ""):
        if self.current_sync is None:
            return

        data = {
            "protocol": self.protocol,
            "msg_type": MSG_CMD,
            "cmd": cmd,
            "src": self.self_id,
            "stamp": now_sec(),
            "reason": reason,
        }

        data.update(self.current_sync.to_dict())

        self.command_pub.publish(
            String(data=json.dumps(data, separators=(",", ":")))
        )

    def _master_abort(self, reason: str):
        if self.current_sync is None:
            self._reset_sync_state()
            return

        rospy.logerr(
            "[SyncGate master] ABORT sync_id=%s reason=%s",
            self.current_sync.sync_id,
            reason,
        )

        self.abort_reason = reason

        self._publish_task_event(
            event_type=EVENT_ABORT,
            action=self.current_sync.action,
            payload=self.current_sync.payload,
            sync_id=self.current_sync.sync_id,
            t0=self.current_sync.t0,
            reason=reason,
        )

        self.sync_state = SYNC_ABORTING
        self.sync_state_start_time = now_sec()
        self.last_command_pub_time = 0.0

        self._publish_command(CMD_ABORT, reason)

    # ============================================================
    # slave 同步状态机
    # ============================================================

    def _tick_slave(self):
        # 从机低频发布全局状态。
        self._publish_global_status_periodically()

        t = now_sec()

        if self.sync_state == SYNC_WAIT_COMMIT and self.current_sync is not None:
            # 只在等待 COMMIT 阶段重复 ACK_PREPARE。
            self._publish_ack_periodically(
                ack_type=ACK_PREPARE,
                accepted=True,
                reason="repeat_ack_prepare",
            )

            if t - self.sync_state_start_time > self.prepare_timeout:
                self._slave_abort_current("wait_commit_timeout")
                return

        elif self.sync_state == SYNC_WAIT_T0 and self.current_sync is not None:
            # 只在等待 t0 阶段重复 ACK_COMMIT。
            self._publish_ack_periodically(
                ack_type=ACK_COMMIT,
                accepted=True,
                reason="repeat_ack_commit",
            )

            if t >= self.current_sync.t0:
                self._publish_task_event(
                    event_type=EVENT_START,
                    action=self.current_sync.action,
                    payload=self.current_sync.payload,
                    sync_id=self.current_sync.sync_id,
                    t0=self.current_sync.t0,
                    reason="start",
                )
                self._close_current_sync()
                self._reset_sync_state()

    def _slave_handle_prepare(self, sync: SyncSession):
        """
        slave 处理 PREPARE。

        关键点：
            PREPARE 阶段不检查 t0。
            因为 PREPARE 消息中的 t0 可能是 0.0。
            PREPARE 只用于询问：你是否 ready 执行 action？
        """
        if self._is_closed_or_old(sync):
            return

        if self.current_sync is not None and self.current_sync.sync_id == sync.sync_id:
            self._publish_ack(
                ack_type=ACK_PREPARE,
                sync=sync,
                accepted=True,
                reason="repeat_ack_prepare_same_sync",
                extra={},
            )
            return

        if self.current_sync is not None and self.current_sync.sync_id != sync.sync_id:
            self._publish_ack(
                ack_type=ACK_PREPARE,
                sync=sync,
                accepted=False,
                reason=f"busy_with:{self.current_sync.sync_id}",
                extra={},
            )
            return

        ok, reason, extra = self._check_local_task_ready_for_action(sync.action)

        if ok:
            # 此时 sync.t0 通常是 0.0。
            # 先保存这次同步会话，等 COMMIT 来了再写入最终 t0。
            self.current_sync = sync
            self.sync_state = SYNC_WAIT_COMMIT
            self.sync_state_start_time = now_sec()
            self.last_ack_pub_time = 0.0

            self._publish_ack(
                ack_type=ACK_PREPARE,
                sync=sync,
                accepted=True,
                reason="ok",
                extra=extra,
            )

            rospy.loginfo(
                "[SyncGate %s] PREPARE accepted action=%s sync_id=%s",
                self.self_id,
                sync.action,
                sync.sync_id,
            )

        else:
            self._publish_ack(
                ack_type=ACK_PREPARE,
                sync=sync,
                accepted=False,
                reason=reason,
                extra=extra,
            )

            rospy.logwarn(
                "[SyncGate %s] PREPARE rejected action=%s reason=%s",
                self.self_id,
                sync.action,
                reason,
            )

    def _slave_handle_commit(self, sync: SyncSession):
        """
        slave 处理 COMMIT。

        关键点：
            COMMIT 阶段必须带最终 t0。
            slave 收到 COMMIT 后，需要把 sync.t0 写入 self.current_sync.t0。
        """
        if self._is_closed_or_old(sync):
            return

        if self.current_sync is None:
            self._publish_ack(
                ack_type=ACK_COMMIT,
                sync=sync,
                accepted=False,
                reason="no_prepared_sync",
                extra={},
            )
            return

        if sync.sync_id != self.current_sync.sync_id:
            self._publish_ack(
                ack_type=ACK_COMMIT,
                sync=sync,
                accepted=False,
                reason="sync_id_mismatch",
                extra={},
            )
            return

        if self.sync_state == SYNC_WAIT_T0:
            self._publish_ack(
                ack_type=ACK_COMMIT,
                sync=self.current_sync,
                accepted=True,
                reason="repeat_ack_commit_same_sync",
                extra={},
            )
            return

        if sync.t0 <= 0:
            self._publish_ack(
                ack_type=ACK_COMMIT,
                sync=sync,
                accepted=False,
                reason="invalid_t0",
                extra={},
            )
            return

        if sync.t0 - now_sec() < self.min_t0_margin:
            self._publish_ack(
                ack_type=ACK_COMMIT,
                sync=sync,
                accepted=False,
                reason="t0_too_close",
                extra={},
            )
            return

        ok, reason, extra = self._check_local_task_ready_for_action(sync.action)

        if ok:
            # 关键点：
            # PREPARE 阶段本地 current_sync.t0 可能还是 0.0。
            # 现在收到 COMMIT，才把最终 t0 写入本地 current_sync。
            self.current_sync.t0 = sync.t0
            self.current_sync.payload = sync.payload
            self.current_sync.participants = sync.participants

            self.sync_state = SYNC_WAIT_T0
            self.sync_state_start_time = now_sec()
            self.last_ack_pub_time = 0.0

            self._publish_task_event(
                event_type=EVENT_SCHEDULED,
                action=self.current_sync.action,
                payload=self.current_sync.payload,
                sync_id=self.current_sync.sync_id,
                t0=self.current_sync.t0,
                reason="scheduled",
            )

            self._publish_ack(
                ack_type=ACK_COMMIT,
                sync=self.current_sync,
                accepted=True,
                reason="ok",
                extra=extra,
            )

            rospy.loginfo(
                "[SyncGate %s] COMMIT accepted action=%s sync_id=%s t0=%.3f",
                self.self_id,
                self.current_sync.action,
                self.current_sync.sync_id,
                self.current_sync.t0,
            )

        else:
            self._publish_ack(
                ack_type=ACK_COMMIT,
                sync=sync,
                accepted=False,
                reason=reason,
                extra=extra,
            )

    def _slave_handle_abort(self, sync: SyncSession, reason: str):
        if self.current_sync is not None and sync.sync_id != self.current_sync.sync_id:
            return

        self._slave_abort_current(f"master_abort:{reason}")

    def _slave_abort_current(self, reason: str):
        if self.current_sync is not None:
            rospy.logerr(
                "[SyncGate %s] ABORT action=%s sync_id=%s reason=%s",
                self.self_id,
                self.current_sync.action,
                self.current_sync.sync_id,
                reason,
            )

            self._publish_task_event(
                event_type=EVENT_ABORT,
                action=self.current_sync.action,
                payload=self.current_sync.payload,
                sync_id=self.current_sync.sync_id,
                t0=self.current_sync.t0,
                reason=reason,
            )

            self._close_current_sync()

        self._reset_sync_state()

    def _check_local_task_ready_for_action(self, action: str) -> Tuple[bool, str, Dict[str, Any]]:
        """
        slave 根据本机 task_fsm_node 状态判断是否可以参与 action。
        """
        if self.local_task_status is None:
            return False, "no_local_task_status", {}

        age = now_sec() - self.local_task_status_time
        if age > self.task_status_timeout:
            return False, f"local_task_status_timeout:{age:.2f}", {}

        ready = bool(self.local_task_status.get("ready", False))
        expected_action = self.local_task_status.get("expected_action", "")

        if self.require_ready_status and not ready:
            return False, self.local_task_status.get("reason", "not_ready"), self.local_task_status

        if expected_action != action:
            return False, f"expected_action:{expected_action},request:{action}", self.local_task_status

        return True, "ok", self.local_task_status

    def _publish_global_status_periodically(self):
        """
        slave 低频发布全局状态给 master。

        该状态只是预检查用。
        真正确认仍然依赖 PREPARE/ACK_PREPARE。
        """
        t = now_sec()
        period = 1.0 / max(self.global_status_rate, 1e-6)

        if t - self.last_global_status_pub_time < period:
            return

        self.last_global_status_pub_time = t

        if self.local_task_status is None:
            ready = False
            expected_action = ""
            reason = "no_local_task_status"
            task_state = ""
        else:
            age = t - self.local_task_status_time
            if age > self.task_status_timeout:
                ready = False
                expected_action = ""
                reason = f"local_task_status_timeout:{age:.2f}"
                task_state = self.local_task_status.get("task_state", "")
            else:
                ready = bool(self.local_task_status.get("ready", False))
                expected_action = self.local_task_status.get("expected_action", "")
                reason = self.local_task_status.get("reason", "")
                task_state = self.local_task_status.get("task_state", "")

        data = {
            "protocol": self.protocol,
            "msg_type": MSG_STATUS,
            "src": self.self_id,
            "stamp": t,
            "ready": ready,
            "reason": reason,
            "expected_action": expected_action,
            "task_state": task_state,
            "sync_state": self.sync_state,
        }

        if self.current_sync is not None:
            data["current_sync"] = self.current_sync.to_dict()

        self.global_status_pub.publish(
            String(data=json.dumps(data, separators=(",", ":")))
        )

    def _publish_ack_periodically(self, ack_type: str, accepted: bool, reason: str):
        """
        ACK 不是一直发，只在对应同步阶段重复发。

        SYNC_WAIT_COMMIT:
            重复 ACK_PREPARE

        SYNC_WAIT_T0:
            重复 ACK_COMMIT
        """
        if self.current_sync is None:
            return

        t = now_sec()
        period = 1.0 / max(self.ack_rate, 1e-6)

        if t - self.last_ack_pub_time < period:
            return

        self.last_ack_pub_time = t

        self._publish_ack(
            ack_type=ack_type,
            sync=self.current_sync,
            accepted=accepted,
            reason=reason,
            extra={},
        )

    def _publish_ack(
        self,
        ack_type: str,
        sync: SyncSession,
        accepted: bool,
        reason: str,
        extra: Dict[str, Any],
    ):
        data = {
            "protocol": self.protocol,
            "msg_type": MSG_ACK,
            "ack_type": ack_type,
            "src": self.self_id,
            "stamp": now_sec(),
            "accepted": bool(accepted),
            "reason": reason,
            "extra": extra if isinstance(extra, dict) else {},
        }

        data.update(sync.to_dict())

        self.ack_pub.publish(
            String(data=json.dumps(data, separators=(",", ":")))
        )

    # ============================================================
    # 公共逻辑
    # ============================================================

    def _is_closed_or_old(self, sync: SyncSession) -> bool:
        if sync.sync_id in self.closed_sync_ids:
            return True

        last_seq = self.last_seq_by_run.get(sync.run_id, 0)
        if sync.sync_seq <= last_seq:
            return True

        return False

    def _close_current_sync(self):
        """
        关闭当前同步会话。

        START 和 ABORT 后都要关闭，避免旧消息影响后续同步。
        """
        if self.current_sync is None:
            return

        sync = self.current_sync

        self.closed_sync_ids.add(sync.sync_id)
        self.closed_sync_order.append(sync.sync_id)

        self.last_seq_by_run[sync.run_id] = max(
            self.last_seq_by_run.get(sync.run_id, 0),
            sync.sync_seq,
        )

        while len(self.closed_sync_order) > self.closed_cache_size:
            old_id = self.closed_sync_order.pop(0)
            self.closed_sync_ids.discard(old_id)

    def _reset_sync_state(self):
        self.current_sync = None
        self.sync_state = SYNC_IDLE
        self.sync_state_start_time = now_sec()
        self.abort_reason = ""
        self.last_command_pub_time = 0.0
        self.last_ack_pub_time = 0.0

    def spin(self):
        rate = rospy.Rate(self.control_rate_hz)

        while not rospy.is_shutdown():
            with self.lock:
                if self.role == "master":
                    self._tick_master()
                else:
                    self._tick_slave()

            rate.sleep()


def main():
    rospy.init_node("sync_gate_node")
    node = SyncGateNode()
    node.spin()


if __name__ == "__main__":
    main()
