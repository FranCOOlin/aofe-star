#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Call trajectory reset services for a three-UAV setup.

Default services:
    /uav0/trajectory/reset
    /uav1/trajectory/reset
    /uav2/trajectory/reset

Usage:
    rosrun aofe_star trajectory_reset_three_uav.py
    rosrun aofe_star trajectory_reset_three_uav.py _vehicle_ids:=uav0,uav1,uav2
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, List, Tuple

import rospy
from std_srvs.srv import Trigger


def _parse_ids(value) -> List[str]:
    if isinstance(value, (list, tuple)):
        raw_items = value
    else:
        raw_items = str(value).replace(",", " ").split()

    vehicle_ids = []
    for item in raw_items:
        vehicle_id = str(item).strip().strip("/")
        if vehicle_id and vehicle_id not in vehicle_ids:
            vehicle_ids.append(vehicle_id)
    return vehicle_ids


class ThreeUavTrajectoryResetClient:
    def __init__(self):
        self.vehicle_ids = _parse_ids(
            rospy.get_param("~vehicle_ids", "uav0,uav1,uav2")
        )
        self.service_suffix = rospy.get_param(
            "~service_suffix",
            "/trajectory/reset",
        )
        self.wait_timeout = float(rospy.get_param("~wait_timeout", 1.0))
        self.parallel = bool(rospy.get_param("~parallel", True))

        if not self.vehicle_ids:
            raise RuntimeError("~vehicle_ids is empty")

    def _service_name(self, vehicle_id: str) -> str:
        return "/" + vehicle_id.strip("/") + "/" + self.service_suffix.strip("/")

    def _call_one(self, vehicle_id: str) -> Tuple[str, str, bool, str]:
        service_name = self._service_name(vehicle_id)
        try:
            rospy.wait_for_service(service_name, timeout=self.wait_timeout)
            resp = rospy.ServiceProxy(service_name, Trigger)()
        except Exception as exc:
            return vehicle_id, service_name, False, str(exc)

        success = bool(getattr(resp, "success", False))
        message = str(getattr(resp, "message", ""))
        return vehicle_id, service_name, success, message

    def call_all(self) -> bool:
        rospy.loginfo(
            "[TrajectoryResetThreeUav] reset vehicles=%s timeout=%.2fs",
            ",".join(self.vehicle_ids),
            self.wait_timeout,
        )

        if self.parallel:
            with ThreadPoolExecutor(max_workers=len(self.vehicle_ids)) as executor:
                futures = [
                    executor.submit(self._call_one, vehicle_id)
                    for vehicle_id in self.vehicle_ids
                ]
                results = [future.result() for future in as_completed(futures)]
        else:
            results = [self._call_one(vehicle_id) for vehicle_id in self.vehicle_ids]

        results.sort(key=lambda item: self.vehicle_ids.index(item[0]))
        all_success = True
        for vehicle_id, service_name, success, message in results:
            if success:
                rospy.loginfo(
                    "[TrajectoryResetThreeUav] %s reset ok service=%s message=%s",
                    vehicle_id,
                    service_name,
                    message,
                )
            else:
                all_success = False
                rospy.logwarn(
                    "[TrajectoryResetThreeUav] %s reset failed service=%s message=%s",
                    vehicle_id,
                    service_name,
                    message,
                )

        return all_success


def main():
    rospy.init_node("trajectory_reset_three_uav")
    client = ThreeUavTrajectoryResetClient()
    success = client.call_all()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
