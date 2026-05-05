#!/usr/bin/python3
#coding=utf-8
import rospy
from nav_msgs.msg import Odometry
from std_msgs.msg import String, Float32
from geometry_msgs.msg import PoseStamped, TwistStamped
from mavros_msgs.msg import State
from sensor_msgs.msg import NavSatFix
from std_srvs.srv import Trigger, TriggerResponse
from math import *
import json
from typing import Optional
# 当收到起飞时间戳(通过geometry_msgs/TwistStamped中的header.stamp,话题:self.uav_id + '/takeoff_time')
# 即得到起飞标志位，发送带有反馈的预设轨迹（位置+速度）。接收主机高度。接收自身高度,接收自身水平位置。发送格式:nav_msgs/Odometry,话题:self.uav_id + '/trajectory' 
# 接收格式:PoseStamped,话题:self.uav_id + '/mavros/local_position/pose'
# 当主机变为位置模式，接收主机位置、速度，打包成相同格式，发送轨迹（位置+速度）。发送格式:nav_msgs/Odometry,话题:self.uav_id + '/trajectory' 不变
# 状态量和期望值都在此文件打包好发给控制器


class Trajectory():
    def __init__(self):
        rospy.init_node('Trajectory_node',anonymous=True)
        rospy.loginfo("the trajectory node init.")

        self.takeoff_flag = False
        self.takeoff_init_finished = False
        self.follow_flag = False
        self.first_init = True
        self.takeoff_start_time = None
        self.action = None
        self.takeoff_duration = 5.0 # 起飞持续时间
        self.adjust_kp = 0.5 # 反馈参数
        self.target_height = 4.0 # 期望高度
        self.trajectory_rate = 80
        self.lon = 0.0
        self.lat = 0.0
        self.Lead_lon = 0.0
        self.Lead_lat = 0.0
        self.offset_x = 0.0
        self.offset_y = 0.0
        self.initial_Lead_yaw = 0.0
        self.Lead_yaw = 0.0
        self.delta_Lead_yaw = 0.0
        self.Follow_yaw = 0.0
        self.initial_pose_x = 0.0
        self.initial_pose_y = 0.0
        self.initial_pose_z = 0.0
        self.initial_pose_x_Leader = 0.0
        self.initial_pose_y_Leader = 0.0
        self.initial_pose_z_Leader = 0.0
        self.Leader_relative_z = 0.0
        self.Follower_relative_z = 0.0
        self.Leader_current_mode = ''
        self.rate = rospy.Rate(self.trajectory_rate)
        self.uav_id = rospy.get_param('~self_id','')
        self.role = rospy.get_param('~role','')
        self.takeoff_param_topic = rospy.get_param(
            "~takeoff_param_topic",
            f"/{self.uav_id}/takeoff/param",
        )

        if self.role == 'master':
            self.uav_id = '/uav0'
        elif self.role == 'slave':
            self.uav_id = '/' + self.uav_id

        self.Leader_pose = PoseStamped()
        self.Follower_pose = PoseStamped()
        self.Leader_vel = TwistStamped()
        self.Follower_vel = TwistStamped()
        self.state_odom = Odometry()
        self.target_odom = Odometry()

        rospy.Subscriber(
            self.takeoff_param_topic,
            String,
            self._takeoff_param_cb,
            queue_size = 50,
        )
        if self.uav_id == '/uav0':
            rospy.Subscriber('/uav0/mavros/state', State, self.Leader_state_callback)
            rospy.Subscriber("/uav0/mavros/local_position/pose", PoseStamped, self.Leader_pose_and_att_callback)
            rospy.Subscriber("/uav0/mavros/local_position/velocity_local", TwistStamped, self.Leader_vel_callback)
        else:
            rospy.Subscriber('/uav0/mavros/state', State, self.Leader_state_callback)
            rospy.Subscriber("/uav0/mavros/local_position/pose", PoseStamped, self.Leader_pose_and_att_callback)
            rospy.Subscriber(self.uav_id + "/mavros/local_position/pose", PoseStamped, self.Follower_pose_and_att_callback)
            rospy.Subscriber("/uav0/mavros/local_position/velocity_local", TwistStamped, self.Leader_vel_callback)
            rospy.Subscriber(self.uav_id + "/mavros/local_position/velocity_local", TwistStamped, self.Follower_vel_callback)
            rospy.Subscriber(self.uav_id + "/mavros/global_position/global", NavSatFix, self.Follower_global_callback)
            rospy.Subscriber("/uav0/mavros/global_position/global", NavSatFix, self.Leader_global_callback)
        self.reset_service = rospy.get_param("~trajectory_reset_service", f"{self.uav_id}/trajectory/reset")
        self.reset_srv = rospy.Service(self.reset_service, Trigger, self._reset_cb)

        self.target_pose_and_velocity_pub = rospy.Publisher(self.uav_id + '/trajectory', Odometry, queue_size=1)
        self.pose_and_velocity_pub = rospy.Publisher(self.uav_id + '/state', Odometry, queue_size=1)
        self.target_yaw_pub = rospy.Publisher(self.uav_id + '/yawd', Float32, queue_size=1)
        self.testpub = rospy.Publisher(self.uav_id + '/deltayaw', Float32, queue_size=1)

    def latlon_delta_to_meters(self, lon_deg_1, lat_deg_1, lon_deg_2, lat_deg_2):
        north_m = (lat_deg_1 - lat_deg_2) * 111320.0
        east_m = (lon_deg_1 - lon_deg_2) * 111320.0 * cos(lat_deg_1 * pi / 180.0)
        return east_m, north_m
    
    def quaternionToEuler(self, q):
        roll  = atan2(2 * (q.w * q.x + q.y * q.z), 1 - 2 * (q.x * q.x + q.y * q.y))
        pitch = asin(2 * (q.w * q.y - q.z * q.x))
        yaw   = atan2(2 * (q.w * q.z + q.x * q.y), 1 - 2 * (q.y * q.y + q.z * q.z))
        return roll, pitch, yaw #弧度

    def safe_json_loads(self, text: str) -> Optional[dict]:
        """
        安全解析 JSON。
        解析失败时返回 None,避免节点直接崩溃。
        """
        try:
            data = json.loads(text)
        except Exception as e:
            rospy.logwarn("json loads failed: %s, raw=%s", str(e), text)
            return None

        if not isinstance(data, dict):
            rospy.logwarn("json root is not dict, raw=%s", text)
            return None

        return data

    def _takeoff_param_cb(self, msg: String):
        """
        接收 JSON 消息。
        """
        data = self.safe_json_loads(msg.data)
        if data is None:
            return

        src = data.get("action", "takeoff")
        stamp = float(data.get("height", 3.0))
        request_type = data.get("duration", 6.0)
        payload = data.get("t0", 1777475562.439756)
        self.action = src
        self.target_height = stamp
        self.takeoff_duration = request_type
        self.takeoff_start_time = payload
        if self.action == "takeoff":
            self.takeoff_flag = True

        rospy.loginfo(
            "received request src=%s stamp=%.3f request_type=%s payload=%s",
            src,
            stamp,
            request_type,
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        )

    def Leader_state_callback(self, data):
        self.Leader_current_mode = data.mode

    def Leader_pose_and_att_callback(self, data):
        self.Leader_pose = data

    def Follower_pose_and_att_callback(self, data):
        self.Follower_pose = data

    def Leader_vel_callback(self, data):
        self.Leader_vel = data
    
    def Follower_vel_callback(self, data):
        self.Follower_vel = data

    def Follower_global_callback(self, data):
        self.lat = data.latitude
        self.lon = data.longitude

    def Leader_global_callback(self, data):
        self.Lead_lat = data.latitude
        self.Lead_lon = data.longitude

    def _reset_cb(self, _req):
        #重置
        self.takeoff_init_finished = False
        self.takeoff_flag = False
        self.follow_flag = False
        rospy.logerr("[%s TrajectoryNode]Trajectory reset requested, resetting internal state.", self.uav_id)
        return TriggerResponse(success=True, message="trajectory reset done")
    
    def set_height(self, time, takeoff_duration, x_stay, y_stay, target_height, Leader_height, Follower_height):
        odom = Odometry()
        t = time
        t_s = t / takeoff_duration #归一化时间
        if t_s >= 1.0:
            odom.pose.pose.position.x = x_stay
            odom.pose.pose.position.y = y_stay
            odom.pose.pose.position.z = target_height + self.adjust_kp * (Leader_height - Follower_height)
            odom.twist.twist.linear.x = 0.0
            odom.twist.twist.linear.y = 0.0
            odom.twist.twist.linear.z = 0.0
        elif t_s < 0.0:
            odom.pose.pose.position.x = x_stay
            odom.pose.pose.position.y = y_stay
            odom.pose.pose.position.z = 0.0
            odom.twist.twist.linear.x = 0.0
            odom.twist.twist.linear.y = 0.0
            odom.twist.twist.linear.z = 0.0
        else:
            odom.pose.pose.position.x = x_stay
            odom.pose.pose.position.y = y_stay
            odom.pose.pose.position.z = target_height * (3*t_s*t_s-2*t_s*t_s*t_s)
            odom.twist.twist.linear.x = 0.0
            odom.twist.twist.linear.y = 0.0
            odom.twist.twist.linear.z = target_height * (6*t_s-6*t_s*t_s) / takeoff_duration
            odom.pose.pose.position.z += self.adjust_kp * (Leader_height - Follower_height)
        return odom

    def start(self):
        if self.uav_id == '/uav0':
            rospy.wait_for_message('/uav0/mavros/local_position/pose', PoseStamped)
            rospy.wait_for_message("/uav0/mavros/local_position/velocity_local", TwistStamped)
            rospy.wait_for_message('/uav0/mavros/state', State)
            rospy.loginfo("Leader trajectory node start.")

            while not rospy.is_shutdown():
                if not self.takeoff_init_finished:
                    self.initial_pose_x_Leader = self.Leader_pose.pose.position.x
                    self.initial_pose_y_Leader = self.Leader_pose.pose.position.y
                    self.initial_pose_z_Leader = self.Leader_pose.pose.position.z
                    self.takeoff_init_finished = True
                    rospy.logerr("[%s TrajectoryNode] Leader initial pose set: x=%.2f, y=%.2f, z=%.2f", self.uav_id, self.initial_pose_x_Leader, self.initial_pose_y_Leader, self.initial_pose_z_Leader)

                self.Leader_relative_z = self.Leader_pose.pose.position.z - self.initial_pose_z_Leader

                if self.takeoff_flag and self.Leader_current_mode == "OFFBOARD": # 当前若是leader,那也只有offboard有意义
                    t = rospy.Time.now().to_sec() - self.takeoff_start_time
                    self.target_odom = self.set_height(t, self.takeoff_duration, self.initial_pose_x_Leader, self.initial_pose_y_Leader, self.target_height, self.Leader_relative_z, self.Leader_relative_z)  # 是Leader时自己减自己相当于不用反馈
                    self.target_odom.header.stamp = rospy.Time.now()
                    self.target_pose_and_velocity_pub.publish(self.target_odom)

                    self.state_odom.pose.pose.position.x = self.Leader_pose.pose.position.x
                    self.state_odom.pose.pose.position.y = self.Leader_pose.pose.position.y
                    self.state_odom.pose.pose.position.z = self.Leader_relative_z
                    self.state_odom.twist.twist.linear.x = self.Leader_vel.twist.linear.x
                    self.state_odom.twist.twist.linear.y = self.Leader_vel.twist.linear.y
                    self.state_odom.twist.twist.linear.z = self.Leader_vel.twist.linear.z
                    self.pose_and_velocity_pub.publish(self.state_odom)

                else:
                    self.target_odom.pose.pose.position.x = self.Leader_pose.pose.position.x
                    self.target_odom.pose.pose.position.y = self.Leader_pose.pose.position.y
                    self.target_odom.pose.pose.position.z = self.Leader_relative_z
                    self.target_odom.twist.twist.linear.x = 0.0
                    self.target_odom.twist.twist.linear.y = 0.0
                    self.target_odom.twist.twist.linear.z = 0.0
                    self.target_odom.header.stamp = rospy.Time.now()
                    self.target_pose_and_velocity_pub.publish(self.target_odom)

                    self.state_odom.pose.pose.position.x = self.Leader_pose.pose.position.x
                    self.state_odom.pose.pose.position.y = self.Leader_pose.pose.position.y
                    self.state_odom.pose.pose.position.z = self.Leader_relative_z
                    self.state_odom.twist.twist.linear.x = self.Leader_vel.twist.linear.x
                    self.state_odom.twist.twist.linear.y = self.Leader_vel.twist.linear.y
                    self.state_odom.twist.twist.linear.z = self.Leader_vel.twist.linear.z
                    self.pose_and_velocity_pub.publish(self.state_odom)
                    
                self.rate.sleep()

        else:
            rospy.wait_for_message('/uav0/mavros/local_position/pose', PoseStamped)
            rospy.wait_for_message(self.uav_id + '/mavros/local_position/pose', PoseStamped)
            rospy.wait_for_message("/uav0/mavros/local_position/velocity_local", TwistStamped)
            rospy.wait_for_message(self.uav_id + '/mavros/local_position/velocity_local', TwistStamped)
            rospy.wait_for_message('/uav0/mavros/state', State)
            rospy.wait_for_message(self.uav_id + "/mavros/global_position/global", NavSatFix)
            rospy.wait_for_message("/uav0/mavros/global_position/global", NavSatFix)
            rospy.loginfo("Follower trajectory node start.")
            
            while not rospy.is_shutdown():
                if not self.takeoff_init_finished:
                    self.initial_pose_x = self.Follower_pose.pose.position.x
                    self.initial_pose_y = self.Follower_pose.pose.position.y
                    self.initial_pose_z = self.Follower_pose.pose.position.z
                    self.initial_pose_z_Leader = self.Leader_pose.pose.position.z
                    if self.first_init:
                        self.offset_x, self.offset_y = self.latlon_delta_to_meters(self.lon, self.lat, self.Lead_lon, self.Lead_lat)
                        roll, pitch, self.initial_Lead_yaw = self.quaternionToEuler(self.Leader_pose.pose.orientation)
                        self.first_init = False
                    rospy.logerr("[%s TrajectoryNode] Follower initial pose set: x=%.2f, y=%.2f, z=%.2f", self.uav_id, self.initial_pose_x, self.initial_pose_y, self.initial_pose_z)
                    self.takeoff_init_finished = True
                
                self.Follower_relative_z = self.Follower_pose.pose.position.z - self.initial_pose_z
                self.Leader_relative_z = self.Leader_pose.pose.position.z - self.initial_pose_z_Leader
                roll, pitch, self.Lead_yaw = self.quaternionToEuler(self.Leader_pose.pose.orientation)
                self.delta_Lead_yaw = self.Lead_yaw - self.initial_Lead_yaw
                self.testpub.publish(self.delta_Lead_yaw)
                self.Follow_yaw = self.Lead_yaw
                self.target_yaw_pub.publish(self.Follow_yaw)

                if self.takeoff_flag and self.Leader_current_mode == "OFFBOARD":
                    t = rospy.Time.now().to_sec() - self.takeoff_start_time
                    self.target_odom = self.set_height(t, self.takeoff_duration, self.initial_pose_x, self.initial_pose_y, self.target_height, self.Leader_relative_z, self.Follower_relative_z) 
                    self.target_odom.header.stamp = rospy.Time.now()
                    self.target_pose_and_velocity_pub.publish(self.target_odom)

                    self.state_odom.pose.pose.position.x = self.Follower_pose.pose.position.x
                    self.state_odom.pose.pose.position.y = self.Follower_pose.pose.position.y
                    self.state_odom.pose.pose.position.z = self.Follower_relative_z
                    self.state_odom.twist.twist.linear.x = self.Follower_vel.twist.linear.x
                    self.state_odom.twist.twist.linear.y = self.Follower_vel.twist.linear.y
                    self.state_odom.twist.twist.linear.z = self.Follower_vel.twist.linear.z
                    self.pose_and_velocity_pub.publish(self.state_odom)
                    self.follow_flag = True
                    
                elif self.Leader_current_mode == "POSCTL" and self.follow_flag:
                    self.target_odom.pose.pose.position.x = self.Leader_pose.pose.position.x + self.offset_x * (cos(self.delta_Lead_yaw) - 1) - self.offset_y * sin(self.delta_Lead_yaw)
                    self.target_odom.pose.pose.position.y = self.Leader_pose.pose.position.y + self.offset_y * (cos(self.delta_Lead_yaw) - 1) + self.offset_x * sin(self.delta_Lead_yaw)
                    self.target_odom.pose.pose.position.z = self.Leader_relative_z
                    self.target_odom.twist.twist.linear.x = self.Leader_vel.twist.linear.x
                    self.target_odom.twist.twist.linear.y = self.Leader_vel.twist.linear.y
                    self.target_odom.twist.twist.linear.z = self.Leader_vel.twist.linear.z
                    self.target_odom.header.stamp = rospy.Time.now()
                    self.target_pose_and_velocity_pub.publish(self.target_odom)

                    self.state_odom.pose.pose.position.x = self.Follower_pose.pose.position.x
                    self.state_odom.pose.pose.position.y = self.Follower_pose.pose.position.y
                    self.state_odom.pose.pose.position.z = self.Follower_relative_z
                    self.state_odom.twist.twist.linear.x = self.Follower_vel.twist.linear.x
                    self.state_odom.twist.twist.linear.y = self.Follower_vel.twist.linear.y
                    self.state_odom.twist.twist.linear.z = self.Follower_vel.twist.linear.z
                    self.pose_and_velocity_pub.publish(self.state_odom)

                else:
                    self.target_odom.pose.pose.position.x = self.Follower_pose.pose.position.x
                    self.target_odom.pose.pose.position.y = self.Follower_pose.pose.position.y
                    self.target_odom.pose.pose.position.z = self.Follower_relative_z
                    self.target_odom.twist.twist.linear.x = 0.0
                    self.target_odom.twist.twist.linear.y = 0.0
                    self.target_odom.twist.twist.linear.z = 0.0
                    self.target_odom.header.stamp = rospy.Time.now()
                    self.target_pose_and_velocity_pub.publish(self.target_odom)

                    self.state_odom.pose.pose.position.x = self.Follower_pose.pose.position.x
                    self.state_odom.pose.pose.position.y = self.Follower_pose.pose.position.y
                    self.state_odom.pose.pose.position.z = self.Follower_relative_z
                    self.state_odom.twist.twist.linear.x = self.Follower_vel.twist.linear.x
                    self.state_odom.twist.twist.linear.y = self.Follower_vel.twist.linear.y
                    self.state_odom.twist.twist.linear.z = self.Follower_vel.twist.linear.z
                    self.pose_and_velocity_pub.publish(self.state_odom)
                    
                self.rate.sleep()

if __name__ == '__main__':
    try:
        traj = Trajectory()
        traj.start()
    except rospy.ROSInterruptException:
        pass


