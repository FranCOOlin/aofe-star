#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fsm_auto_test.py

用途：
    自动测试 task_fsm_node.py + sync_gate_node.py 的完整任务流程。

测试流程：
    1. 等待所有飞机 mission/state 出现；
    2. 发布 master start；
    3. 等待所有飞机进入 TASK_TAKEOFF_READY；
    4. 发布 /operator/takeoff；
    5. 等待所有飞机进入 TASK_WAIT_ENTER_FOLLOW_CMD；
    6. 发布 /operator/enter_follow；
    7. 等待 master 进入 TASK_MASTER_PILOT_HOLD，slave 进入 TASK_FOLLOW_MASTER；
    8. 发布 /operator/hook_sequence；
    9. 等待松钩/收绳阶段结束，回到主从飞行阶段；
    10. 发布 /operator/land；
    11. 等待 master 回到 TASK_IDLE，slave 回到 TASK_SELF_CHECK 或 TASK_TAKEOFF_READY。

同时监听并打印：
    /sync/status
    /sync/command
    /sync/ack
    /<id>/mission/state
    /<id>/task/sync_status
    /<id>/task/sync_request
    /<id>/task/sync_event
    /<id>/planner/request
    /<id>/controller/command
    /<id>/hook/command

运行示例：
    rosrun aofe_star fsm_auto_test.py _master_id:=master _vehicle_ids:=master,uav4

如果 master 是 uav1：
    rosrun aofe_star fsm_auto_test.py _master_id:=uav1 _vehicle_ids:=uav1,uav4
"""

import json
import threading
from typing import Any, Dict, List, Optional

import rospy
from std_msgs.msg import String, Bool


# ============================================================
# 工具函数
# ============================================================

def now_sec() -> float:
    return rospy.Time.now().to_sec()


def parse_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, str):
        return [x.strip() for x in value.split(",") if x.strip()]
    return []


def safe_json_loads(text: str) -> Optional[dict]:
    try:
        return json.loads(text)
    except Exception:
        return None


def compact_json(data: dict) -> str:
    try:
        return json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return str(data)


# ============================================================
# 自动测试节点
# ============================================================

class FsmAutoTest:
    def __init__(self):
        # -----------------------------
        # 参数
        # -----------------------------
        self.master_id = rospy.get_param("~master_id", "master")
        self.vehicle_ids = parse_list(rospy.get_param("~vehicle_ids", "master,uav4"))

        if self.master_id not in self.vehicle_ids:
            self.vehicle_ids.insert(0, self.master_id)

        self.slave_ids = [x for x in self.vehicle_ids if x != self.master_id]

        self.print_all = bool(rospy.get_param("~print_all", False))
        self.print_controller_throttle = float(
            rospy.get_param("~print_controller_throttle", 1.0)
        )

        self.step_timeout = float(rospy.get_param("~step_timeout", 60.0))
        self.takeoff_timeout = float(rospy.get_param("~takeoff_timeout", 30.0))
        self.hook_timeout = float(rospy.get_param("~hook_timeout", 20.0))
        self.land_timeout = float(rospy.get_param("~land_timeout", 60.0))

        self.command_repeat_count = int(rospy.get_param("~command_repeat_count", 3))
        self.command_repeat_dt = float(rospy.get_param("~command_repeat_dt", 0.2))

        # 是否自动执行完整流程。
        # false 时只监听打印，不自动发指令。
        self.auto_run = bool(rospy.get_param("~auto_run", True))

        # 是否测试 emergency hold。
        # 默认 false，因为 emergency hold 会打断完整流程。
        self.test_emergency_hold = bool(rospy.get_param("~test_emergency_hold", False))

        # -----------------------------
        # 状态缓存
        # -----------------------------
        self.lock = threading.RLock()

        self.mission_state: Dict[str, dict] = {}
        self.sync_status: Dict[str, dict] = {}
        self.task_sync_status: Dict[str, dict] = {}
        self.task_sync_event: Dict[str, dict] = {}
        self.task_sync_request: Dict[str, dict] = {}
        self.controller_command: Dict[str, dict] = {}
        self.planner_request: Dict[str, dict] = {}
        self.hook_command: Dict[str, dict] = {}

        self.last_print_key: Dict[str, str] = {}
        self.last_controller_print_time: Dict[str, float] = {}

        # -----------------------------
        # Publishers
        # -----------------------------
        self.pub_master_start = rospy.Publisher(
            f"/{self.master_id}/task/start",
            Bool,
            queue_size=5,
        )

        self.pub_operator_takeoff = rospy.Publisher(
            "/operator/takeoff",
            Bool,
            queue_size=5,
        )

        self.pub_operator_enter_follow = rospy.Publisher(
            "/operator/enter_follow",
            Bool,
            queue_size=5,
        )

        self.pub_operator_hook_sequence = rospy.Publisher(
            "/operator/hook_sequence",
            Bool,
            queue_size=5,
        )

        self.pub_operator_land = rospy.Publisher(
            "/operator/land",
            Bool,
            queue_size=5,
        )

        self.pub_operator_emergency_hold = rospy.Publisher(
            "/operator/emergency_hold",
            Bool,
            queue_size=5,
        )

        # -----------------------------
        # Subscribers: 全局同步协议
        # -----------------------------
        rospy.Subscriber("/sync/status", String, self._sync_status_cb, queue_size=100)
        rospy.Subscriber("/sync/command", String, self._generic_print_cb("/sync/command"), queue_size=100)
        rospy.Subscriber("/sync/ack", String, self._generic_print_cb("/sync/ack"), queue_size=100)

        # -----------------------------
        # Subscribers: 每台机 FSM / 控制 / 钩子
        # -----------------------------
        for vid in self.vehicle_ids:
            rospy.Subscriber(
                f"/{vid}/mission/state",
                String,
                self._mission_state_cb_factory(vid),
                queue_size=100,
            )

            rospy.Subscriber(
                f"/{vid}/task/sync_status",
                String,
                self._task_sync_status_cb_factory(vid),
                queue_size=100,
            )

            rospy.Subscriber(
                f"/{vid}/task/sync_request",
                String,
                self._task_sync_request_cb_factory(vid),
                queue_size=100,
            )

            rospy.Subscriber(
                f"/{vid}/task/sync_event",
                String,
                self._task_sync_event_cb_factory(vid),
                queue_size=100,
            )

            rospy.Subscriber(
                f"/{vid}/planner/request",
                String,
                self._planner_request_cb_factory(vid),
                queue_size=100,
            )

            rospy.Subscriber(
                f"/{vid}/controller/command",
                String,
                self._controller_command_cb_factory(vid),
                queue_size=100,
            )

            rospy.Subscriber(
                f"/{vid}/hook/command",
                String,
                self._hook_command_cb_factory(vid),
                queue_size=100,
            )

        rospy.loginfo(
            "[FsmAutoTest] master_id=%s vehicle_ids=%s slave_ids=%s auto_run=%s",
            self.master_id,
            self.vehicle_ids,
            self.slave_ids,
            self.auto_run,
        )

    # ============================================================
    # 回调与打印
    # ============================================================

    def _print(self, tag: str, data: Any):
        stamp = now_sec()

        if isinstance(data, dict):
            text = compact_json(data)
        else:
            text = str(data)

        rospy.loginfo("[%.3f] %s %s", stamp, tag, text)

    def _print_on_change(self, key: str, tag: str, data: dict, field_list: List[str]):
        """
        只在关键字段变化时打印，避免 50Hz 控制命令刷屏。
        print_all=true 时每条都打印。
        """
        if self.print_all:
            self._print(tag, data)
            return

        parts = []
        for f in field_list:
            parts.append(f"{f}={data.get(f, '')}")

        signature = "|".join(parts)
        old = self.last_print_key.get(key, "")

        if signature != old:
            self.last_print_key[key] = signature
            self._print(tag, data)

    def _generic_print_cb(self, topic: str):
        def cb(msg: String):
            data = safe_json_loads(msg.data)
            if data is None:
                self._print(topic, msg.data)
            else:
                self._print(topic, data)
        return cb

    def _sync_status_cb(self, msg: String):
        data = safe_json_loads(msg.data)
        if not data:
            self._print("/sync/status/raw", msg.data)
            return

        src = data.get("src", "unknown")

        with self.lock:
            self.sync_status[src] = data

        self._print_on_change(
            key=f"sync_status:{src}",
            tag="/sync/status",
            data=data,
            field_list=["src", "ready", "expected_action", "task_state", "sync_state"],
        )

    def _mission_state_cb_factory(self, vid: str):
        def cb(msg: String):
            data = safe_json_loads(msg.data)
            if not data:
                self._print(f"/{vid}/mission/state/raw", msg.data)
                return

            with self.lock:
                self.mission_state[vid] = data

            self._print_on_change(
                key=f"mission:{vid}",
                tag=f"/{vid}/mission/state",
                data=data,
                field_list=[
                    "task_state",
                    "expected_sync_action",
                    "current_action",
                    "current_sync_id",
                    "emergency_hold",
                ],
            )
        return cb

    def _task_sync_status_cb_factory(self, vid: str):
        def cb(msg: String):
            data = safe_json_loads(msg.data)
            if not data:
                self._print(f"/{vid}/task/sync_status/raw", msg.data)
                return

            with self.lock:
                self.task_sync_status[vid] = data

            self._print_on_change(
                key=f"task_sync_status:{vid}",
                tag=f"/{vid}/task/sync_status",
                data=data,
                field_list=["task_state", "ready", "expected_action", "current_action"],
            )
        return cb

    def _task_sync_request_cb_factory(self, vid: str):
        def cb(msg: String):
            data = safe_json_loads(msg.data)
            if not data:
                self._print(f"/{vid}/task/sync_request/raw", msg.data)
                return

            with self.lock:
                self.task_sync_request[vid] = data

            self._print(f"/{vid}/task/sync_request", data)
        return cb

    def _task_sync_event_cb_factory(self, vid: str):
        def cb(msg: String):
            data = safe_json_loads(msg.data)
            if not data:
                self._print(f"/{vid}/task/sync_event/raw", msg.data)
                return

            with self.lock:
                self.task_sync_event[vid] = data

            self._print(f"/{vid}/task/sync_event", data)
        return cb

    def _planner_request_cb_factory(self, vid: str):
        def cb(msg: String):
            data = safe_json_loads(msg.data)
            if not data:
                self._print(f"/{vid}/planner/request/raw", msg.data)
                return

            with self.lock:
                self.planner_request[vid] = data

            self._print(f"/{vid}/planner/request", data)
        return cb

    def _controller_command_cb_factory(self, vid: str):
        def cb(msg: String):
            data = safe_json_loads(msg.data)
            if not data:
                self._print(f"/{vid}/controller/command/raw", msg.data)
                return

            with self.lock:
                self.controller_command[vid] = data

            # 控制器命令可能 50Hz，默认只在 mode/task_state 变化时打印；
            # 另外每 print_controller_throttle 秒补打一条。
            key = f"controller:{vid}"
            mode = data.get("mode", "")
            task_state = data.get("task_state", "")
            signature = f"mode={mode}|task_state={task_state}"

            last_sig = self.last_print_key.get(key, "")
            t = now_sec()
            last_t = self.last_controller_print_time.get(key, 0.0)

            if self.print_all or signature != last_sig or t - last_t >= self.print_controller_throttle:
                self.last_print_key[key] = signature
                self.last_controller_print_time[key] = t
                self._print(f"/{vid}/controller/command", data)
        return cb

    def _hook_command_cb_factory(self, vid: str):
        def cb(msg: String):
            data = safe_json_loads(msg.data)
            if not data:
                self._print(f"/{vid}/hook/command/raw", msg.data)
                return

            with self.lock:
                self.hook_command[vid] = data

            self._print(f"/{vid}/hook/command", data)
        return cb

    # ============================================================
    # 自动发布命令
    # ============================================================

    def publish_bool_repeated(self, pub: rospy.Publisher, topic_name: str, value: bool = True):
        rospy.loginfo("[FsmAutoTest] publish %s = %s", topic_name, value)

        # 等待连接不是绝对必要，但可以减少第一条消息丢失概率。
        rospy.sleep(0.2)

        for _ in range(self.command_repeat_count):
            pub.publish(Bool(data=value))
            rospy.sleep(self.command_repeat_dt)

    def cmd_start_master(self):
        self.publish_bool_repeated(
            self.pub_master_start,
            f"/{self.master_id}/task/start",
            True,
        )

    def cmd_takeoff(self):
        self.publish_bool_repeated(
            self.pub_operator_takeoff,
            "/operator/takeoff",
            True,
        )

    def cmd_enter_follow(self):
        self.publish_bool_repeated(
            self.pub_operator_enter_follow,
            "/operator/enter_follow",
            True,
        )

    def cmd_hook_sequence(self):
        self.publish_bool_repeated(
            self.pub_operator_hook_sequence,
            "/operator/hook_sequence",
            True,
        )

    def cmd_land(self):
        self.publish_bool_repeated(
            self.pub_operator_land,
            "/operator/land",
            True,
        )

    def cmd_emergency_hold(self):
        self.publish_bool_repeated(
            self.pub_operator_emergency_hold,
            "/operator/emergency_hold",
            True,
        )

    # ============================================================
    # 状态等待工具
    # ============================================================

    def get_state(self, vid: str) -> str:
        with self.lock:
            return self.mission_state.get(vid, {}).get("task_state", "")

    def get_all_states(self) -> Dict[str, str]:
        with self.lock:
            return {
                vid: self.mission_state.get(vid, {}).get("task_state", "")
                for vid in self.vehicle_ids
            }

    def wait_until(self, desc: str, condition_fn, timeout: float) -> bool:
        rospy.loginfo("[FsmAutoTest] WAIT: %s timeout=%.1fs", desc, timeout)

        start = now_sec()
        last_report = 0.0
        rate = rospy.Rate(10)

        while not rospy.is_shutdown():
            if condition_fn():
                rospy.loginfo("[FsmAutoTest] PASS: %s states=%s", desc, self.get_all_states())
                return True

            t = now_sec()
            if t - start > timeout:
                rospy.logerr("[FsmAutoTest] TIMEOUT: %s states=%s", desc, self.get_all_states())
                return False

            if t - last_report > 2.0:
                last_report = t
                rospy.loginfo("[FsmAutoTest] waiting %s states=%s", desc, self.get_all_states())

            rate.sleep()

        return False

    def all_have_state(self, state: str) -> bool:
        states = self.get_all_states()
        return all(states.get(vid, "") == state for vid in self.vehicle_ids)

    def all_in_states(self, allowed: List[str]) -> bool:
        states = self.get_all_states()
        return all(states.get(vid, "") in allowed for vid in self.vehicle_ids)

    def master_is(self, state: str) -> bool:
        return self.get_state(self.master_id) == state

    def slaves_are(self, state: str) -> bool:
        return all(self.get_state(sid) == state for sid in self.slave_ids)

    def follow_stage_ready(self) -> bool:
        return (
            self.master_is("TASK_MASTER_PILOT_HOLD")
            and self.slaves_are("TASK_FOLLOW_MASTER")
        )

    def wait_mission_state_appeared(self) -> bool:
        return self.wait_until(
            desc="all mission/state appeared",
            condition_fn=lambda: all(self.get_state(vid) != "" for vid in self.vehicle_ids),
            timeout=self.step_timeout,
        )

    # ============================================================
    # 自动测试主流程
    # ============================================================

    def run_auto_flow(self):
        rospy.loginfo("[FsmAutoTest] auto flow start")

        if not self.wait_mission_state_appeared():
            return

        # 1. 启动 master
        self.cmd_start_master()

        # 2. 等待所有飞机进入 TASK_TAKEOFF_READY
        ok = self.wait_until(
            desc="all vehicles reach TASK_TAKEOFF_READY",
            condition_fn=lambda: self.all_have_state("TASK_TAKEOFF_READY"),
            timeout=self.step_timeout,
        )
        if not ok:
            return

        # 3. 触发同步起飞
        self.cmd_takeoff()

        # 4. 等待起飞完成，进入 TASK_WAIT_ENTER_FOLLOW_CMD
        ok = self.wait_until(
            desc="all vehicles reach TASK_WAIT_ENTER_FOLLOW_CMD after synced takeoff",
            condition_fn=lambda: self.all_have_state("TASK_WAIT_ENTER_FOLLOW_CMD"),
            timeout=self.takeoff_timeout,
        )
        if not ok:
            return

        # 5. 进入主从阶段
        self.cmd_enter_follow()

        ok = self.wait_until(
            desc="master in TASK_MASTER_PILOT_HOLD and slaves in TASK_FOLLOW_MASTER",
            condition_fn=self.follow_stage_ready,
            timeout=self.step_timeout,
        )
        if not ok:
            return

        # 6. 松钩 + 5s 后收绳
        self.cmd_hook_sequence()

        # 先等待进入 hook 状态，避免命令没生效。
        ok = self.wait_until(
            desc="all vehicles enter TASK_HOOK_SEQUENCE_RUNNING",
            condition_fn=lambda: self.all_have_state("TASK_HOOK_SEQUENCE_RUNNING"),
            timeout=5.0,
        )
        if not ok:
            return

        # 再等待回到主从阶段。
        ok = self.wait_until(
            desc="hook sequence finished and back to follow stage",
            condition_fn=self.follow_stage_ready,
            timeout=self.hook_timeout,
        )
        if not ok:
            return

        # 7. 同步降落
        self.cmd_land()

        ok = self.wait_until(
            desc="all vehicles enter TASK_LAND_RUNNING",
            condition_fn=lambda: self.all_have_state("TASK_LAND_RUNNING"),
            timeout=self.step_timeout,
        )
        if not ok:
            return

        # 8. 等待复位
        ok = self.wait_until(
            desc="mission reset after landing",
            condition_fn=lambda: (
                self.master_is("TASK_IDLE")
                and all(self.get_state(sid) in ["TASK_SELF_CHECK", "TASK_TAKEOFF_READY"] for sid in self.slave_ids)
            ),
            timeout=self.land_timeout + 10.0,
        )
        if not ok:
            return

        rospy.loginfo("[FsmAutoTest] AUTO FLOW PASSED")

        if self.test_emergency_hold:
            rospy.loginfo("[FsmAutoTest] testing emergency hold after flow")
            self.cmd_emergency_hold()

    def spin(self):
        rospy.sleep(1.0)

        if self.auto_run:
            self.run_auto_flow()

        rospy.loginfo("[FsmAutoTest] now only listening. Ctrl-C to exit.")
        rospy.spin()


def main():
    rospy.init_node("fsm_auto_test")
    node = FsmAutoTest()
    node.spin()


if __name__ == "__main__":
    main()



# 运行方式

# 先启动你的两台机的 sync_stack.launch。

# 例如 master：

# roslaunch aofe_star sync_stack.launch \
#     role:=master \
#     self_id:=master \
#     participants:=uav4 \
#     run_id:=fsm_test_001 \
#     demo_mode:=true

# 从机 uav4：

# roslaunch aofe_star sync_stack.launch \
#     role:=slave \
#     self_id:=uav4 \
#     participants:=uav4 \
#     run_id:=fsm_test_001 \
#     demo_mode:=true

# 然后在 master 机器另开终端运行测试节点：

# source ~/catkin_ws/devel/setup.bash

# rosrun aofe_star fsm_auto_test.py \
#     _master_id:=master \
#     _vehicle_ids:=master,uav4 \
#     _auto_run:=true

# 如果 master 是 uav1，从机是 uav4：

# rosrun aofe_star fsm_auto_test.py \
#     _master_id:=uav1 \
#     _vehicle_ids:=uav1,uav4 \
#     _auto_run:=true
# 只监听不自动发布

# 如果你只想看打印，不想让它自动发起流程：

# rosrun aofe_star fsm_auto_test.py \
#     _master_id:=master \
#     _vehicle_ids:=master,uav4 \
#     _auto_run:=false
# 打印所有消息

# 默认为了避免 /controller/command 高频刷屏，我做了“变化时打印 + 定时补打”。如果你想每条都打印：

# rosrun aofe_star fsm_auto_test.py \
#     _master_id:=master \
#     _vehicle_ids:=master,uav4 \
#     _auto_run:=true \
#     _print_all:=true