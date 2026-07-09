#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Terminal dashboard for task_fsm_node.

It watches /<uav_id>/mission/state JSON messages and renders a compact monitor
view. It is intentionally read-only and never publishes commands.
"""

import argparse
import json
import sys
from collections import deque
from typing import Any, Dict, Iterable, List, Optional

import rospy
from std_msgs.msg import String

try:
    from rich.console import Console, Group
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table

    HAVE_RICH = True
except ImportError:
    HAVE_RICH = False


STALE_STYLE = "red"
WARN_STYLE = "yellow"
OK_STYLE = "green"


def parse_csv(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(part).strip() for part in value if str(part).strip()]
    return [part.strip() for part in str(value).split(",") if part.strip()]


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def bool_text(value: Any) -> str:
    return "Y" if bool(value) else "-"


def color(text: str, style: str, enabled: bool = True) -> str:
    if not enabled or not HAVE_RICH:
        return text
    return f"[{style}]{text}[/{style}]"


class TaskFSMMonitor:
    def __init__(self):
        args = self._parse_args(rospy.myargv()[1:])

        self.master_id = args.master_id
        self.vehicle_ids = parse_csv(args.vehicles)
        if self.master_id not in self.vehicle_ids:
            self.vehicle_ids.insert(0, self.master_id)

        self.refresh_hz = max(args.refresh_hz, 0.5)
        self.state_timeout = args.state_timeout
        self.max_events = max(args.max_events, 1)
        self.use_rich = HAVE_RICH and not args.plain
        self.screen = args.screen

        self.states: Dict[str, Dict[str, Any]] = {}
        self.last_state_by_vehicle: Dict[str, str] = {}
        self.last_action_by_vehicle: Dict[str, str] = {}
        self.events = deque(maxlen=self.max_events)

        for vehicle_id in self.vehicle_ids:
            topic = f"/{vehicle_id}/mission/state"
            rospy.Subscriber(
                topic,
                String,
                self._mission_state_cb,
                callback_args=vehicle_id,
                queue_size=20,
            )

        if self.use_rich:
            self.console = Console()
        else:
            self.console = None

        if not HAVE_RICH and not args.plain:
            rospy.logwarn(
                "[task_fsm_monitor] rich is not installed; using plain terminal output."
            )

        rospy.loginfo(
            "[task_fsm_monitor] master=%s vehicles=%s mode=monitor_only",
            self.master_id,
            ",".join(self.vehicle_ids),
        )

    @staticmethod
    def _parse_args(argv: List[str]):
        parser = argparse.ArgumentParser(
            description="Render a terminal dashboard for task_fsm_node mission states."
        )
        parser.add_argument(
            "--master-id",
            default=str(rospy.get_param("~master_id", "uav0")),
            help="Master vehicle id. Default: uav0",
        )
        parser.add_argument(
            "--vehicles",
            default=str(rospy.get_param("~vehicles", "uav0,uav1,uav2")),
            help="Comma-separated vehicle ids. Default: uav0,uav1,uav2",
        )
        parser.add_argument(
            "--refresh-hz",
            type=float,
            default=float(rospy.get_param("~refresh_hz", 4.0)),
            help="Dashboard refresh rate. Default: 4.0",
        )
        parser.add_argument(
            "--state-timeout",
            type=float,
            default=float(rospy.get_param("~state_timeout", 2.0)),
            help="Warn when a mission state is older than this many seconds.",
        )
        parser.add_argument(
            "--max-events",
            type=int,
            default=int(rospy.get_param("~max_events", 10)),
            help="Number of recent events to show. Default: 10",
        )
        parser.add_argument(
            "--plain",
            action="store_true",
            default=bool(rospy.get_param("~plain", False)),
            help="Use plain terminal output even if rich is installed.",
        )
        parser.add_argument(
            "--screen",
            action="store_true",
            default=bool(rospy.get_param("~screen", False)),
            help="Use rich alternate screen buffer.",
        )
        return parser.parse_args(argv)

    def _mission_state_cb(self, msg: String, vehicle_id: str):
        try:
            data = json.loads(msg.data)
        except Exception:
            return
        if not isinstance(data, dict):
            return

        data["_topic"] = f"/{vehicle_id}/mission/state"
        data["_recv_time"] = rospy.Time.now().to_sec()
        self.states[vehicle_id] = data

        state = str(data.get("task_state", "UNKNOWN"))
        old_state = self.last_state_by_vehicle.get(vehicle_id)
        if state and state != old_state:
            self.last_state_by_vehicle[vehicle_id] = state
            self._add_event(f"{vehicle_id} state -> {state}")

        action = str(data.get("current_action", ""))
        old_action = self.last_action_by_vehicle.get(vehicle_id)
        if action and action != old_action:
            self.last_action_by_vehicle[vehicle_id] = action
            self._add_event(f"{vehicle_id} action -> {action}")

    def _add_event(self, text: str):
        self.events.append((rospy.Time.now().to_sec(), text))

    def _fresh_state(self, vehicle_id: str) -> Optional[Dict[str, Any]]:
        state = self.states.get(vehicle_id)
        if not state:
            return None
        stamp = safe_float(state.get("stamp"))
        age = rospy.Time.now().to_sec() - stamp
        if stamp <= 0.0 or age > self.state_timeout:
            return None
        return state

    def _style_state(self, state: str) -> str:
        if state in ("TASK_ABORTED", "TASK_EMERGENCY_HOLD"):
            return color(state, "bold red", self.use_rich)
        if state in ("TASK_LAND_RUNNING", "TASK_RESETTING"):
            return color(state, WARN_STYLE, self.use_rich)
        if state in ("TASK_FOLLOW_MASTER", "TASK_MASTER_PILOT_HOLD"):
            return color(state, OK_STYLE, self.use_rich)
        if state in ("TASK_TAKEOFF_RUNNING", "TASK_HOOK_SEQUENCE_RUNNING"):
            return color(state, "cyan", self.use_rich)
        return state

    def _state_age_text(self, payload: Optional[Dict[str, Any]]) -> str:
        if payload is None:
            return color("stale", STALE_STYLE, self.use_rich)
        now = rospy.Time.now().to_sec()
        age = now - safe_float(payload.get("stamp"))
        recv_age = now - safe_float(payload.get("_recv_time"), now)
        if age > self.state_timeout or recv_age > self.state_timeout:
            return color(f"{age:.1f}s", STALE_STYLE, self.use_rich)
        return f"{age:.1f}s"

    def _sync_text(self, payload: Dict[str, Any]) -> str:
        expected = str(payload.get("expected_sync_action") or "-")
        current = str(payload.get("current_action") or "-")
        sync_id = str(payload.get("current_sync_id") or "-")
        request = bool_text(payload.get("request_in_flight"))
        return f"exp={expected} cur={current} req={request} id={sync_id}"

    def _schedule_text(self, payload: Dict[str, Any]) -> str:
        if not payload.get("scheduled"):
            return "-"
        t0 = safe_float(payload.get("current_t0"))
        if t0 <= 0.0:
            return "Y"
        dt = t0 - rospy.Time.now().to_sec()
        return f"Y t0{dt:+.1f}s"

    def _hook_text(self, payload: Dict[str, Any]) -> str:
        return "rel={} ret={}".format(
            bool_text(payload.get("hook_released")),
            bool_text(payload.get("rope_retracted")),
        )

    def _offboard_text(self, payload: Dict[str, Any]) -> str:
        permission = bool_text(payload.get("rc_offboard_permission"))
        source = str(payload.get("offboard_permission_source") or "-")
        switch_value = payload.get("offboard_switch_value")
        need_offboard = bool_text(payload.get("need_offboard"))
        need_arm = bool_text(payload.get("need_arm"))
        if switch_value is None:
            switch_part = "sw=-"
        else:
            switch_part = f"sw={safe_float(switch_value):.0f}"
        return f"ok={permission} {source} {switch_part} need(O/A)={need_offboard}/{need_arm}"

    def _rc_task_text(self, payload: Dict[str, Any]) -> str:
        return "tk={} hk={} ld={} em={}".format(
            bool_text(payload.get("rc_task_takeoff_active")),
            bool_text(payload.get("rc_task_hook_active")),
            bool_text(payload.get("rc_task_land_active")),
            bool_text(payload.get("rc_emergency_hold_active")),
        )

    def _manual_text(self, payload: Dict[str, Any]) -> str:
        return "SB={} OB={} TK={} SA={} SD={}".format(
            payload.get("manual_control_sb_bits", "--"),
            bool_text(payload.get("manual_control_offboard")),
            bool_text(payload.get("manual_control_takeoff")),
            bool_text(payload.get("manual_control_sa")),
            bool_text(payload.get("manual_control_sd")),
        )

    def _self_check_text(self, payload: Dict[str, Any]) -> str:
        report = payload.get("self_check") or {}
        if not report:
            return "-"
        if report.get("bypass"):
            return color("bypass", WARN_STYLE, self.use_rich)
        if report.get("ok"):
            return color("ok", OK_STYLE, self.use_rich)

        failed = []
        for key in ("mavros", "gps", "ekf", "rc"):
            value = report.get(key) or {}
            if value and not value.get("ok", False):
                fresh = "fresh" if value.get("fresh", False) else "stale"
                failed.append(f"{key}:{fresh}")
        if not failed:
            return color("bad", WARN_STYLE, self.use_rich)
        return color(",".join(failed), WARN_STYLE, self.use_rich)

    def _render_status_table(self):
        title = "Task FSM Monitor [read-only]"
        table = Table(title=title, expand=True)
        table.add_column("UAV", no_wrap=True)
        table.add_column("Role", no_wrap=True)
        table.add_column("Task State", no_wrap=True)
        table.add_column("Age", justify="right", no_wrap=True)
        table.add_column("Sync")
        table.add_column("Sched", no_wrap=True)
        table.add_column("Hook", no_wrap=True)
        table.add_column("Offboard")
        table.add_column("RC Task", no_wrap=True)
        table.add_column("Manual")
        table.add_column("Self Check")

        for vehicle_id in self.vehicle_ids:
            payload = self._fresh_state(vehicle_id)
            raw_payload = self.states.get(vehicle_id)
            if payload is None:
                task_state = "NO_FRESH_STATE"
                if raw_payload:
                    task_state = str(raw_payload.get("task_state", task_state))
                table.add_row(
                    vehicle_id,
                    "-",
                    color(task_state, STALE_STYLE, self.use_rich),
                    self._state_age_text(payload),
                    "-",
                    "-",
                    "-",
                    "-",
                    "-",
                    "-",
                    "-",
                )
                continue

            table.add_row(
                vehicle_id,
                str(payload.get("role", "-")),
                self._style_state(str(payload.get("task_state", "UNKNOWN"))),
                self._state_age_text(payload),
                self._sync_text(payload),
                self._schedule_text(payload),
                self._hook_text(payload),
                self._offboard_text(payload),
                self._rc_task_text(payload),
                self._manual_text(payload),
                self._self_check_text(payload),
            )
        return table

    def _render_summary_panel(self):
        table = Table.grid(expand=True)
        table.add_column(justify="left", ratio=1)
        table.add_column(justify="left", ratio=4)

        fresh_count = sum(1 for vehicle_id in self.vehicle_ids if self._fresh_state(vehicle_id))
        table.add_row("vehicles", f"{fresh_count}/{len(self.vehicle_ids)} fresh")
        table.add_row("topics", ", ".join(f"/{vehicle_id}/mission/state" for vehicle_id in self.vehicle_ids))
        table.add_row("mode", "monitor only; no publishers, no command shortcuts")
        return Panel(table, title="Monitor")

    def _render_events_table(self):
        table = Table(title="Recent Events", expand=True)
        table.add_column("Age", justify="right", no_wrap=True)
        table.add_column("Event")
        now = rospy.Time.now().to_sec()
        for stamp, event in list(self.events)[-self.max_events:]:
            table.add_row(f"{now - stamp:.1f}s", event)
        return table

    def _render_rich(self):
        return Group(
            self._render_status_table(),
            self._render_summary_panel(),
            self._render_events_table(),
        )

    def _plain_lines(self) -> Iterable[str]:
        now = rospy.Time.now().to_sec()
        yield "Task FSM Monitor [read-only]"
        yield "Mode: monitor only; no publishers, no command shortcuts"
        yield ""
        for vehicle_id in self.vehicle_ids:
            payload = self._fresh_state(vehicle_id)
            if payload is None:
                raw_payload = self.states.get(vehicle_id)
                state = "NO_FRESH_STATE"
                age = "stale"
                if raw_payload:
                    state = str(raw_payload.get("task_state", state))
                    age = f"{now - safe_float(raw_payload.get('stamp')):.1f}s"
                yield f"{vehicle_id}: {state} age={age}"
                continue

            yield (
                "{uav}: role={role} state={state} age={age} sync=[{sync}] "
                "sched={sched} hook=[{hook}] offboard=[{offboard}] rc=[{rc}] "
                "manual=[{manual}] self_check={self_check}"
            ).format(
                uav=vehicle_id,
                role=payload.get("role", "-"),
                state=payload.get("task_state", "UNKNOWN"),
                age=self._state_age_text(payload),
                sync=self._sync_text(payload),
                sched=self._schedule_text(payload),
                hook=self._hook_text(payload),
                offboard=self._offboard_text(payload),
                rc=self._rc_task_text(payload),
                manual=self._manual_text(payload),
                self_check=self._self_check_text(payload),
            )

        yield ""
        yield "Recent Events:"
        for stamp, event in list(self.events)[-self.max_events:]:
            yield f"  {now - stamp:.1f}s  {event}"

    def _render_plain_once(self):
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.write("\n".join(self._plain_lines()))
        sys.stdout.write("\n")
        sys.stdout.flush()

    def spin(self):
        rate = rospy.Rate(self.refresh_hz)
        if self.use_rich:
            with Live(
                self._render_rich(),
                console=self.console,
                refresh_per_second=self.refresh_hz,
                screen=self.screen,
            ) as live:
                while not rospy.is_shutdown():
                    live.update(self._render_rich())
                    rate.sleep()
        else:
            while not rospy.is_shutdown():
                self._render_plain_once()
                rate.sleep()


def main():
    rospy.init_node("task_fsm_monitor")
    TaskFSMMonitor().spin()


if __name__ == "__main__":
    main()
