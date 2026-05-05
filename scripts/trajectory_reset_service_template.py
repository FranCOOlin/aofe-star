#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Template service for trajectory generator reset.

TaskFSM calls this service while it is in TASK_SELF_CHECK:
    /<self_id>/trajectory/reset

Return TriggerResponse(success=True) after the trajectory generator has
cleared all per-flight state. If success=False or the call times out, TaskFSM
will retry while it remains in TASK_SELF_CHECK.
"""

import rospy
from std_srvs.srv import Trigger, TriggerResponse


class TrajectoryResetServiceTemplate:
    def __init__(self):
        self.self_id = rospy.get_param("~self_id", "uav1")
        self.reset_service = rospy.get_param(
            "~trajectory_reset_service",
            f"/{self.self_id}/trajectory/reset",
        )

        self.reset_count = 0
        self.reset_srv = rospy.Service(
            self.reset_service,
            Trigger,
            self._reset_cb,
        )

        rospy.loginfo(
            "[TrajectoryResetServiceTemplate] self_id=%s service=%s",
            self.self_id,
            self.reset_service,
        )

    def _reset_cb(self, _req):
        self.reset_count += 1

        # TODO: replace this block with real trajectory-generator reset:
        # - clear active trajectory/action id
        # - clear cached initial pose/t0
        # - clear per-flight integrators/flags
        # - prepare for the next takeoff plan

        rospy.loginfo(
            "[TrajectoryResetServiceTemplate] reset accepted count=%d",
            self.reset_count,
        )
        return TriggerResponse(success=True, message="trajectory reset done")


def main():
    rospy.init_node("trajectory_reset_service_template")
    TrajectoryResetServiceTemplate()
    rospy.spin()


if __name__ == "__main__":
    main()
