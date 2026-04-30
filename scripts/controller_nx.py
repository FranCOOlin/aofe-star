#!/usr/bin/python3
#coding=utf-8
import rospy
from nav_msgs.msg import Odometry
from geometry_msgs.msg import Twist
from mavros_msgs.msg import State
import numpy as np

class PID_Controller():
    def __init__(self, kp, ki, kd, output_MAX, int_i_MAX):
        self.kp = kp
        self.ki = ki
        self.kd = kd
        self.output_MAX = output_MAX
        self.int_i_MAX = int_i_MAX

        self.err = None
        self.int_i = 0.0
        self.dif_d = 0.0
        self.output = 0.0
        self.vec_err = None
        self.vec_int_i = None
        self.vec_dif_d = None
        self.vec_output = None

        self.t_now = rospy.Time.now().to_sec()
        self.t_last = rospy.Time.now().to_sec()

    def calculation(self, ref, fdb, D_by_state = False , dref = None, dfdb = None):
        if isinstance(ref, np.ndarray):
            self.vector_cal(ref, fdb, D_by_state, dref, dfdb)
        else:
            self.scalar_cal(ref, fdb, D_by_state, dref, dfdb)
    
    def scalar_cal(self, ref, fdb, D_by_state = False , dref = None, dfdb = None):
        if self.err is None:
            self.err = [0, 0]
            self.int_i = 0
            self.dif_d = 0
            self.output = 0
        self.err[1] = self.err[0]
        self.err[0] = ref - fdb

        self.t_now = rospy.Time.now().to_sec()
        dt = self.t_now - self.t_last
        if dt > 0.2 or dt < 1e-4:
            self.int_i = 0
            self.dif_d = 0
        else:
            self.int_i = self.int_i + (self.err[0] + self.err[1])/2 * dt
            if not D_by_state:
                self.dif_d = (self.err[0] - self.err[1]) / dt
            else:
                self.dif_d = dref - dfdb
        self.t_last = self.t_now

        if self.int_i < -self.int_i_MAX:
            self.int_i = -self.int_i_MAX
        elif self.int_i > self.int_i_MAX:
            self.int_i = self.int_i_MAX

        self.output = self.kp * self.err[0] + self.ki * self.int_i + self.kd * self.dif_d

        if self.output < -self.output_MAX:
            self.output = -self.output_MAX
        elif self.output > self.output_MAX:
            self.output = self.output_MAX
    
    def vector_cal(self, ref, fdb, D_by_state = False , dref = None, dfdb = None):
        if self.vec_err is None:
            shape = np.shape(ref)
            self.vec_err = [np.zeros(shape), np.zeros(shape)]
            self.vec_int_i = np.zeros(shape)
            self.vec_dif_d = np.zeros(shape)
            self.vec_output = np.zeros(shape)
        self.vec_err[1] = self.vec_err[0].copy()
        self.vec_err[0] = ref - fdb

        self.t_now = rospy.Time.now().to_sec()
        dt = self.t_now - self.t_last
        if dt > 0.2 or dt < 1e-4:
            shape = np.shape(ref)
            self.vec_int_i = np.zeros(shape)
            self.vec_dif_d = np.zeros(shape)
        else:
            self.vec_int_i = self.vec_int_i + (self.vec_err[0] + self.vec_err[1])/2 * dt
            if not D_by_state:
                self.vec_dif_d = (self.vec_err[0] - self.vec_err[1]) / dt
            else:
                self.vec_dif_d = dref - dfdb
        self.t_last = self.t_now
        self.vec_int_i = np.clip(self.vec_int_i, -self.int_i_MAX, self.int_i_MAX)
        self.vec_output = self.kp * self.vec_err[0] + self.ki * self.vec_int_i + self.kd * self.vec_dif_d
        self.vec_output = np.clip(self.vec_output, -self.output_MAX, self.output_MAX)
        

class Control_loop():
    def __init__(self):
        rospy.init_node('Controller_node', anonymous=True)
        rospy.loginfo("the control node init.")
        self.control_rate = 80
        self.rate = rospy.Rate(self.control_rate)
        self.uav_id = rospy.get_param('~self_id','')
        self.role = rospy.get_param('~role','')

        if self.role == 'master':
            self.uav_id = '/master'
        elif self.role == 'slave':
            self.uav_id = '/' + self.uav_id

        self.traj_xy = np.zeros(2)
        self.traj_z = 0
        self.traj_vel_xy = np.zeros(2)
        self.traj_vel_z = 0
        self.Follower_xy = np.zeros(2)
        self.Follower_z = 0
        self.Follower_vel_xy = np.zeros(2)
        self.Follower_vel_z = 0
        self.Control_to_vel = Twist()

        self.traj_ready = False
        self.state_ready = False

        rospy.Subscriber(self.uav_id + '/trajectory', Odometry, self.traj_callback)
        rospy.Subscriber(self.uav_id + '/state', Odometry, self.state_callback)
        rospy.Subscriber(self.uav_id + '/mavros/state', State, self.Leader_state_callback)
        self.setpoint_velocity_cmd_vel_pub = rospy.Publisher(self.uav_id + '/mavros/setpoint_velocity/cmd_vel_unstamped', Twist, queue_size = 1)

    def traj_callback(self, data):
        self.traj_xy = np.array([data.pose.pose.position.x, data.pose.pose.position.y])
        self.traj_z = data.pose.pose.position.z
        self.traj_vel_xy = np.array([data.twist.twist.linear.x, data.twist.twist.linear.y])
        self.traj_vel_z = data.twist.twist.linear.z
        self.traj_ready = True

    def state_callback(self, data):
        self.Follower_xy = np.array([data.pose.pose.position.x, data.pose.pose.position.y])
        self.Follower_z = data.pose.pose.position.z
        self.Follower_vel_xy = np.array([data.twist.twist.linear.x, data.twist.twist.linear.y])
        self.Follower_vel_z = data.twist.twist.linear.z
        self.state_ready = True

    def Leader_state_callback(self, data):
        self.current_mode = data.mode

    def Run(self):
        # PID_horizon = PID_Controller(1.4, 0.0, 0.1, 5.0, 10)
        # PID_height = PID_Controller(0.4, 0.01, 0, 1, 10) #不要差分,第三项一定要置为0
        PID_horizon = PID_Controller(1.4, 0.0, 0.1, 0.5, 10)
        PID_height = PID_Controller(0.4, 0.01, 0, 0.5, 5) #不要差分,第三项一定要置为0,测试用限制
        while not rospy.is_shutdown():
            if self.traj_ready and self.state_ready:
                PID_horizon.calculation(self.traj_xy, self.Follower_xy, True, self.traj_vel_xy, self.Follower_vel_xy)
                PID_height.calculation(self.traj_z, self.Follower_z, True, self.traj_vel_z, self.Follower_vel_z)
                self.Control_to_vel.linear.x = PID_horizon.vec_output[0]
                self.Control_to_vel.linear.y = PID_horizon.vec_output[1]
                self.Control_to_vel.linear.z = PID_height.output
                if self.current_mode == 'OFFBOARD': # 只有在当前无人机为板载模式下才可以取代原有位置控制器！
                    self.setpoint_velocity_cmd_vel_pub.publish(self.Control_to_vel)
            self.rate.sleep()

if __name__ == '__main__':
    try:
        control_loop = Control_loop()
        control_loop.Run()
    except rospy.ROSInterruptException:
        pass