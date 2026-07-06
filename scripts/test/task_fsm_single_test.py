#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
task_fsm_single_test.py

单机测试 task_fsm_node.py + sync_gate_node.py 的 master 流程。

本机同步接口已改为 ROS service，测试节点不再旁路监听
/<self_id>/task/sync_status、sync_request、sync_event。

典型用法：
    roslaunch aofe_star sys_coop_lift_test_001_master_sim.launch demo_mode:=true

    rosrun aofe_star task_fsm_single_test.py --self-id uav0 --auto-run

手动模式：
    rosrun aofe_star task_fsm_single_test.py --self-id uav0
    回车发布当前状态下的下一条合适指令。
"""

import argparse
import json
import select
import sys
from typing import Dict, List, Optional

import rospy
from std_msgs.msg import Bool, String


TASK_IDLE = "TASK_IDLE"
TASK_SELF_CHECK = "TASK_SELF_CHECK"
TASK_TAKEOFF_READY = "TASK_TAKEOFF_READY"
TASK_TAKEOFF_RUNNING = "TASK_TAKEOFF_RUNNING"
TASK_WAIT_ENTER_FOLLOW_CMD = "TASK_WAIT_ENTER_FOLLOW_CMD"
TASK_MASTER_PILOT_HOLD = "TASK_MASTER_PILOT_HOLD"
TASK_HOOK_SEQUENCE_RUNNING = "TASK_HOOK_SEQUENCE_RUNNING"
TASK_LAND_RUNNING = "TASK_LAND_RUNNING"
TASK_RESETTING = "TASK_RESETTING"
TASK_EMERGENCY_HOLD = "TASK_EMERGENCY_HOLD"
TASK_ABORTED = "TASK_ABORTED"


def safe_json_loads(text: str) -> Optional[dict]:
    try:
        data = json.loads(text)
    except Exception:
        return None
    return data if isinstance(data, dict) else None


class TaskFSMSingleTest:
    def __init__(self):
        args = self._parse_args(rospy.myargv()[1:])

        self.self_id = args.self_id
        self.state_timeout = args.state_timeout
        self.auto_run = args.auto_run
        self.skip_hook = args.skip_hook

        self.step_timeout = args.step_timeout
        self.takeoff_timeout = args.takeoff_timeout
        self.hook_timeout = args.hook_timeout
        self.land_timeout = args.land_timeout

        self.command_repeat_count = args.command_repeat_count
        self.command_repeat_dt = args.command_repeat_dt

        self.state: Optional[dict] = None
        self.last_print_key: Dict[str, str] = {}

        self.start_pub = rospy.Publisher(
            f"/{self.self_id}/task/start",
            Bool,
            queue_size=5,
        )
        self.takeoff_pub = rospy.Publisher("/operator/takeoff", Bool, queue_size=5)
        self.enter_follow_pub = rospy.Publisher(
            "/operator/enter_follow",
            Bool,
            queue_size=5,
        )
        self.hook_pub = rospy.Publisher(
            "/operator/hook_sequence",
            Bool,
            queue_size=5,
        )
        self.land_pub = rospy.Publisher("/operator/land", Bool, queue_size=5)
        self.emergency_pub = rospy.Publisher(
            "/operator/emergency_hold",
            Bool,
            queue_size=5,
        )

        rospy.Subscriber(
            f"/{self.self_id}/mission/state",
            String,
            self._mission_state_cb,
            queue_size=100,
        )
        rospy.Subscriber(
            f"/{self.self_id}/planner/request",
            String,
            self._print_json_cb(f"/{self.self_id}/planner/request"),
            queue_size=100,
        )
        rospy.Subscriber(
            f"/{self.self_id}/hook/command",
            String,
            self._print_json_cb(f"/{self.self_id}/hook/command"),
            queue_size=100,
        )

    @staticmethod
    def _parse_args(argv: List[str]):
        parser = argparse.ArgumentParser(
            description="Single-machine TaskFSM test/operator for a master vehicle."
        )
        parser.add_argument(
            "--self-id",
            default="uav0",
            help="Vehicle id to test. Default: uav0",
        )
        parser.add_argument(
            "--auto-run",
            action="store_true",
            help="Run the full single-master flow automatically.",
        )
        parser.add_argument(
            "--state-timeout",
            type=float,
            default=2.0,
            help="Mission state freshness timeout in seconds. Default: 2.0",
        )
        parser.add_argument(
            "--step-timeout",
            type=float,
            default=30.0,
            help="Generic step timeout in seconds. Default: 30.0",
        )
        parser.add_argument(
            "--takeoff-timeout",
            type=float,
            default=30.0,
            help="Takeoff completion timeout in seconds. Default: 30.0",
        )
        parser.add_argument(
            "--hook-timeout",
            type=float,
            default=20.0,
            help="Hook sequence timeout in seconds. Default: 20.0",
        )
        parser.add_argument(
            "--land-timeout",
            type=float,
            default=45.0,
            help="Landing and reset timeout in seconds. Default: 45.0",
        )
        parser.add_argument(
            "--command-repeat-count",
            type=int,
            default=3,
            help="How many times to repeat each Bool command. Default: 3",
        )
        parser.add_argument(
            "--command-repeat-dt",
            type=float,
            default=0.2,
            help="Seconds between repeated Bool commands. Default: 0.2",
        )
        parser.add_argument(
            "--skip-hook",
            action="store_true",
            help="Skip /operator/hook_sequence and go directly to land.",
        )
        return parser.parse_args(argv)

    def _mission_state_cb(self, msg: String):
        data = safe_json_loads(msg.data)
        if data is None:
            rospy.logwarn("Invalid mission state: %s", msg.data)
            return

        self.state = data
        self._print_json_on_change(
            f"/{self.self_id}/mission/state",
            data,
            [
                "task_state",
                "expected_sync_action",
                "scheduled",
                "current_action",
                "current_sync_id",
                "hook_released",
                "rope_retracted",
            ],
        )

    def _print_json_cb(self, tag: str):
        def cb(msg: String):
            data = safe_json_loads(msg.data)
            if data is None:
                rospy.loginfo("%s %s", tag, msg.data)
            else:
                rospy.loginfo("%s %s", tag, self._compact_json(data))

        return cb

    def _print_json_on_change_cb(self, tag: str, fields: List[str]):
        def cb(msg: String):
            data = safe_json_loads(msg.data)
            if data is None:
                rospy.loginfo("%s %s", tag, msg.data)
                return
            self._print_json_on_change(tag, data, fields)

        return cb

    def _print_json_on_change(self, tag: str, data: dict, fields: List[str]):
        signature = "|".join(f"{field}={data.get(field, '')}" for field in fields)
        if self.last_print_key.get(tag, "") == signature:
            return
        self.last_print_key[tag] = signature
        rospy.loginfo("%s %s", tag, self._compact_json(data))

    @staticmethod
    def _compact_json(data: dict) -> str:
        try:
            return json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        except Exception:
            return str(data)

    def spin(self):
        rospy.loginfo(
            "[TaskFSMSingleTest] self_id=%s auto_run=%s skip_hook=%s",
            self.self_id,
            self.auto_run,
            self.skip_hook,
        )

        rospy.sleep(1.0)
        if self.auto_run:
            self.run_auto_flow()
            rospy.loginfo("[TaskFSMSingleTest] auto flow finished; now listening.")
            rospy.spin()
            return

        self._print_help()
        rate = rospy.Rate(10)
        while not rospy.is_shutdown():
            if self._stdin_ready():
                line = sys.stdin.readline()
                if line == "":
                    break
                cmd = line.strip().lower()
                if cmd in ["q", "quit", "exit"]:
                    break
                if cmd in ["s", "status"]:
                    self._print_status()
                elif cmd in ["e", "emergency"]:
                    self._publish(self.emergency_pub, "/operator/emergency_hold")
                elif cmd in ["h", "help", "?"]:
                    self._print_help()
                else:
                    self.publish_next_command()
            rate.sleep()

    @staticmethod
    def _stdin_ready() -> bool:
        readable, _, _ = select.select([sys.stdin], [], [], 0.0)
        return bool(readable)

    def _fresh_state(self) -> Optional[dict]:
        if not self.state:
            return None
        stamp = float(self.state.get("stamp", 0.0))
        if stamp <= 0.0 or rospy.Time.now().to_sec() - stamp > self.state_timeout:
            return None
        return self.state

    def _state_name(self) -> str:
        state = self._fresh_state()
        if state is None:
            return "NO_FRESH_STATE"
        return str(state.get("task_state", "UNKNOWN"))

    def _hook_done(self) -> bool:
        state = self._fresh_state()
        if not state:
            return False
        return bool(state.get("hook_released", False)) and bool(
            state.get("rope_retracted", False)
        )

    def publish_next_command(self):
        state = self._state_name()

        if state == "NO_FRESH_STATE":
            rospy.logwarn("No fresh mission state from %s yet.", self.self_id)
            self._print_status()
            return

        if state == TASK_IDLE:
            self._publish(self.start_pub, f"/{self.self_id}/task/start")
            return

        if state == TASK_SELF_CHECK:
            if self.wait_state(TASK_TAKEOFF_READY, self.step_timeout):
                self.publish_next_command()
            return

        if state == TASK_TAKEOFF_READY:
            self._publish(self.takeoff_pub, "/operator/takeoff")
            return

        if state == TASK_TAKEOFF_RUNNING:
            if self.wait_state(TASK_WAIT_ENTER_FOLLOW_CMD, self.takeoff_timeout):
                self.publish_next_command()
            return

        if state == TASK_WAIT_ENTER_FOLLOW_CMD:
            self._publish(self.enter_follow_pub, "/operator/enter_follow")
            return

        if state == TASK_MASTER_PILOT_HOLD:
            if self.skip_hook or self._hook_done():
                self._publish(self.land_pub, "/operator/land")
            else:
                self._publish(self.hook_pub, "/operator/hook_sequence")
            return

        if state == TASK_HOOK_SEQUENCE_RUNNING:
            if self.wait_state(TASK_MASTER_PILOT_HOLD, self.hook_timeout):
                self.publish_next_command()
            return

        if state == TASK_LAND_RUNNING:
            rospy.loginfo("Land is running. No further operator command needed.")
            self._print_status()
            return

        if state in [TASK_RESETTING, TASK_ABORTED, TASK_EMERGENCY_HOLD]:
            rospy.logwarn("Current state does not accept a normal next command: %s", state)
            self._print_status()
            return

        rospy.logwarn("No suitable next command for state: %s", state)
        self._print_status()

    def run_auto_flow(self):
        rospy.loginfo("[TaskFSMSingleTest] auto flow start")

        if not self.wait_until(
            "mission/state appeared",
            lambda: self._state_name() != "NO_FRESH_STATE",
            self.step_timeout,
        ):
            return

        if self._state_name() == TASK_IDLE:
            self._publish(self.start_pub, f"/{self.self_id}/task/start")

        if not self.wait_state(TASK_TAKEOFF_READY, self.step_timeout):
            return

        self._publish(self.takeoff_pub, "/operator/takeoff")
        if not self.wait_state(TASK_WAIT_ENTER_FOLLOW_CMD, self.takeoff_timeout):
            return

        self._publish(self.enter_follow_pub, "/operator/enter_follow")
        if not self.wait_state(TASK_MASTER_PILOT_HOLD, self.step_timeout):
            return

        if not self.skip_hook:
            self._publish(self.hook_pub, "/operator/hook_sequence")
            if not self.wait_state(TASK_HOOK_SEQUENCE_RUNNING, 5.0):
                return
            if not self.wait_state(TASK_MASTER_PILOT_HOLD, self.hook_timeout):
                return

        self._publish(self.land_pub, "/operator/land")
        if not self.wait_state(TASK_LAND_RUNNING, self.step_timeout):
            return

        if not self.wait_until(
            "mission reset after landing",
            lambda: self._state_name() in [TASK_IDLE, TASK_SELF_CHECK, TASK_TAKEOFF_READY],
            self.land_timeout,
        ):
            return

        rospy.loginfo("[TaskFSMSingleTest] AUTO FLOW PASSED")

    def wait_state(self, wanted: str, timeout: float) -> bool:
        return self.wait_until(
            f"{self.self_id} reaches {wanted}",
            lambda: self._state_name() == wanted,
            timeout,
        )

    def wait_until(self, desc: str, condition_fn, timeout: float) -> bool:
        rospy.loginfo("[TaskFSMSingleTest] WAIT: %s timeout=%.1fs", desc, timeout)

        start = rospy.Time.now().to_sec()
        last_report = 0.0
        rate = rospy.Rate(10)

        while not rospy.is_shutdown():
            if condition_fn():
                rospy.loginfo(
                    "[TaskFSMSingleTest] PASS: %s state=%s",
                    desc,
                    self._state_name(),
                )
                return True

            now = rospy.Time.now().to_sec()
            if now - start > timeout:
                rospy.logerr(
                    "[TaskFSMSingleTest] TIMEOUT: %s state=%s",
                    desc,
                    self._state_name(),
                )
                return False

            if now - last_report > 2.0:
                last_report = now
                rospy.loginfo(
                    "[TaskFSMSingleTest] waiting %s state=%s",
                    desc,
                    self._state_name(),
                )

            rate.sleep()

        return False

    def _publish(self, pub: rospy.Publisher, topic: str):
        rospy.loginfo("[TaskFSMSingleTest] publish std_msgs/Bool true to %s", topic)
        rospy.sleep(0.2)
        for _ in range(self.command_repeat_count):
            pub.publish(Bool(data=True))
            rospy.sleep(self.command_repeat_dt)

    def _print_status(self):
        state = self._fresh_state()
        if state is None:
            print(f"{self.self_id}: NO_FRESH_STATE")
        else:
            print(
                "{}: {} expected={} scheduled={} action={} sync_id={} hook={} rope={}".format(
                    self.self_id,
                    state.get("task_state", "UNKNOWN"),
                    state.get("expected_sync_action", ""),
                    state.get("scheduled", False),
                    state.get("current_action", ""),
                    state.get("current_sync_id", ""),
                    state.get("hook_released", False),
                    state.get("rope_retracted", False),
                )
            )
        sys.stdout.flush()

    @staticmethod
    def _print_help():
        print(
            "\nTaskFSM single-machine test/operator\n"
            "  Enter : publish the next suitable command\n"
            "  s     : print current mission state\n"
            "  e     : publish /operator/emergency_hold\n"
            "  q     : quit\n"
        )
        sys.stdout.flush()


def main():
    rospy.init_node("task_fsm_single_test")
    node = TaskFSMSingleTest()
    node.spin()


if __name__ == "__main__":
    main()
