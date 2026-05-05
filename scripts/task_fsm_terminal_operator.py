#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
TASK_FOLLOW_MASTER = "TASK_FOLLOW_MASTER"
TASK_HOOK_SEQUENCE_RUNNING = "TASK_HOOK_SEQUENCE_RUNNING"
TASK_LAND_RUNNING = "TASK_LAND_RUNNING"
TASK_RESETTING = "TASK_RESETTING"
TASK_EMERGENCY_HOLD = "TASK_EMERGENCY_HOLD"
TASK_ABORTED = "TASK_ABORTED"


class TaskFSMTerminalOperator:
    def __init__(self):
        args = self._parse_args(rospy.myargv()[1:])

        self.master_id = args.master_id
        self.vehicle_ids = [v.strip() for v in args.vehicles.split(",") if v.strip()]
        if self.master_id not in self.vehicle_ids:
            self.vehicle_ids.insert(0, self.master_id)

        self.slave_ids = [v for v in self.vehicle_ids if v != self.master_id]
        self.state_timeout = args.state_timeout
        self.step_timeout = args.step_timeout
        self.takeoff_timeout = args.takeoff_timeout
        self.hook_timeout = args.hook_timeout
        self.skip_hook = args.skip_hook

        self.states: Dict[str, dict] = {}

        self.start_pub = rospy.Publisher(
            f"/{self.master_id}/task/start",
            Bool,
            queue_size=1,
        )
        self.takeoff_pub = rospy.Publisher("/operator/takeoff", Bool, queue_size=1)
        self.enter_follow_pub = rospy.Publisher(
            "/operator/enter_follow",
            Bool,
            queue_size=1,
        )
        self.hook_pub = rospy.Publisher(
            "/operator/hook_sequence",
            Bool,
            queue_size=1,
        )
        self.land_pub = rospy.Publisher("/operator/land", Bool, queue_size=1)
        self.emergency_pub = rospy.Publisher(
            "/operator/emergency_hold",
            Bool,
            queue_size=1,
        )

        for vid in self.vehicle_ids:
            rospy.Subscriber(
                f"/{vid}/mission/state",
                String,
                self._mission_state_cb,
                callback_args=vid,
                queue_size=20,
            )

    @staticmethod
    def _parse_args(argv: List[str]):
        parser = argparse.ArgumentParser(
            description="Press Enter to publish the next suitable TaskFSM operator command."
        )
        parser.add_argument(
            "--master-id",
            default="uav0",
            help="Master vehicle id. Default: uav0",
        )
        parser.add_argument(
            "--vehicles",
            default="uav0,uav1,uav2",
            help="Comma-separated vehicle ids to monitor. Default: uav0,uav1,uav2",
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
            help="Generic wait timeout in seconds. Default: 30.0",
        )
        parser.add_argument(
            "--takeoff-timeout",
            type=float,
            default=30.0,
            help="Takeoff wait timeout in seconds. Default: 30.0",
        )
        parser.add_argument(
            "--hook-timeout",
            type=float,
            default=20.0,
            help="Hook sequence wait timeout in seconds. Default: 20.0",
        )
        parser.add_argument(
            "--skip-hook",
            action="store_true",
            help="Skip /operator/hook_sequence and go to land after follow stage.",
        )
        return parser.parse_args(argv)

    def _mission_state_cb(self, msg: String, vid: str):
        try:
            data = json.loads(msg.data)
        except Exception:
            return
        if not isinstance(data, dict):
            return
        self.states[vid] = data

    def spin(self):
        rospy.loginfo(
            "[TaskFSMTerminalOperator] master=%s vehicles=%s",
            self.master_id,
            ",".join(self.vehicle_ids),
        )
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

    def _fresh_state(self, vid: str) -> Optional[dict]:
        state = self.states.get(vid)
        if not state:
            return None
        stamp = float(state.get("stamp", 0.0))
        if stamp <= 0.0 or rospy.Time.now().to_sec() - stamp > self.state_timeout:
            return None
        return state

    def _all_fresh(self) -> bool:
        return all(self._fresh_state(vid) is not None for vid in self.vehicle_ids)

    def _state_name(self, vid: str) -> str:
        state = self._fresh_state(vid)
        if not state:
            return "NO_FRESH_STATE"
        return str(state.get("task_state", "UNKNOWN"))

    def _states_are(self, wanted: str) -> bool:
        return self._all_fresh() and all(
            self._state_name(vid) == wanted for vid in self.vehicle_ids
        )

    def _follow_stage_ready(self) -> bool:
        return (
            self._state_name(self.master_id) == TASK_MASTER_PILOT_HOLD
            and all(
                self._state_name(vid) == TASK_FOLLOW_MASTER
                for vid in self.slave_ids
            )
        )

    def _hook_done(self) -> bool:
        for vid in self.vehicle_ids:
            state = self._fresh_state(vid)
            if not state:
                continue
            if bool(state.get("hook_released", False)) and bool(
                state.get("rope_retracted", False)
            ):
                return True
        return False

    def publish_next_command(self):
        master_state = self._state_name(self.master_id)

        if master_state == "NO_FRESH_STATE":
            rospy.logwarn("No fresh mission state from master %s yet.", self.master_id)
            self._print_status()
            return

        if master_state == TASK_IDLE:
            self._publish(self.start_pub, f"/{self.master_id}/task/start")
            return

        if not self._all_fresh():
            rospy.logwarn("Waiting for fresh mission states from all vehicles.")
            self._print_status()
            return

        if self._states_are(TASK_SELF_CHECK):
            if self.wait_until(
                "all vehicles reach TASK_TAKEOFF_READY",
                lambda: self._states_are(TASK_TAKEOFF_READY),
                self.step_timeout,
            ):
                self.publish_next_command()
            return

        if self._states_are(TASK_TAKEOFF_READY):
            self._publish(self.takeoff_pub, "/operator/takeoff")
            return

        if self._states_are(TASK_TAKEOFF_RUNNING):
            if self.wait_until(
                "all vehicles leave takeoff running",
                lambda: self._states_are(TASK_WAIT_ENTER_FOLLOW_CMD)
                or self._follow_stage_ready(),
                self.takeoff_timeout,
            ):
                self.publish_next_command()
            return

        if self._states_are(TASK_WAIT_ENTER_FOLLOW_CMD):
            self._publish(self.enter_follow_pub, "/operator/enter_follow")
            return

        if self._follow_stage_ready():
            if self.skip_hook or self._hook_done():
                self._publish(self.land_pub, "/operator/land")
            else:
                self._publish(self.hook_pub, "/operator/hook_sequence")
            return

        if any(
            self._state_name(vid) == TASK_HOOK_SEQUENCE_RUNNING
            for vid in self.vehicle_ids
        ):
            if self.wait_until(
                "hook sequence returns to follow stage",
                self._follow_stage_ready,
                self.hook_timeout,
            ):
                self.publish_next_command()
            return

        if any(self._state_name(vid) == TASK_LAND_RUNNING for vid in self.vehicle_ids):
            rospy.loginfo("Land is running. No further operator command needed.")
            self._print_status()
            return

        if any(
            self._state_name(vid) in [TASK_RESETTING, TASK_ABORTED, TASK_EMERGENCY_HOLD]
            for vid in self.vehicle_ids
        ):
            rospy.logwarn("At least one vehicle is resetting, aborted, or emergency hold.")
            self._print_status()
            return

        rospy.logwarn("No suitable next command for the current mixed states.")
        self._print_status()

    @staticmethod
    def _publish(pub: rospy.Publisher, topic: str):
        msg = Bool(data=True)
        for _ in range(3):
            pub.publish(msg)
            rospy.sleep(0.05)
        rospy.loginfo("Published std_msgs/Bool true to %s", topic)

    def wait_until(self, desc: str, condition_fn, timeout: float) -> bool:
        rospy.loginfo("[TaskFSMTerminalOperator] WAIT: %s timeout=%.1fs", desc, timeout)
        start = rospy.Time.now().to_sec()
        last_report = 0.0
        rate = rospy.Rate(10)

        while not rospy.is_shutdown():
            if condition_fn():
                rospy.loginfo("[TaskFSMTerminalOperator] PASS: %s", desc)
                return True

            now = rospy.Time.now().to_sec()
            if now - start > timeout:
                rospy.logwarn("[TaskFSMTerminalOperator] TIMEOUT: %s", desc)
                self._print_status()
                return False

            if now - last_report > 2.0:
                last_report = now
                self._print_status()

            rate.sleep()

        return False

    def _print_status(self):
        rows = []
        for vid in self.vehicle_ids:
            state = self._fresh_state(vid)
            if state is None:
                rows.append(f"{vid}: NO_FRESH_STATE")
                continue
            rows.append(
                "{}: {} expected={} scheduled={} action={}".format(
                    vid,
                    state.get("task_state", "UNKNOWN"),
                    state.get("expected_sync_action", ""),
                    state.get("scheduled", False),
                    state.get("current_action", ""),
                )
            )
        print("\n".join(rows))
        sys.stdout.flush()

    @staticmethod
    def _print_help():
        print(
            "\nTaskFSM terminal operator\n"
            "  Enter : publish the next suitable command\n"
            "  s     : print current mission states\n"
            "  e     : publish /operator/emergency_hold\n"
            "  q     : quit\n"
        )
        sys.stdout.flush()


def main():
    rospy.init_node("task_fsm_terminal_operator")
    node = TaskFSMTerminalOperator()
    node.spin()


if __name__ == "__main__":
    main()
