#!/usr/bin/python3
#coding=utf-8
import rospy
from nav_msgs.msg import Odometry
from std_msgs.msg import Float32
from geometry_msgs.msg import Twist
from mavros_msgs.msg import PositionTarget, State
import numpy as np

class Lp_filter(): #没用到,先写着,暂时一阶
    def __init__(self, wc):
        self.wc = wc #截止频率
        self.enable = True
        if self.wc == 0:
            self.enable = False
        else:
            self.tau = 1 / self.wc
            self.control_rate = 80
            self.period = 1 / self.control_rate
            self.alpha = self.period / (self.tau + self.period)
        self.output = 0.0
        
    def calculate(self, new_val):
        if self.enable == True:
            self.output =  (1 - self.alpha) * self.output + self.alpha * new_val
        else:
            self.output = new_val
        return self.output

class PID_Controller():
    def __init__(self, kp, ki, kd, output_MAX, int_i_MAX, MAXERR_TOINT):
        self.kp = kp
        self.ki = ki
        self.kd = kd
        self.output_MAX = output_MAX
        self.int_i_MAX = int_i_MAX
        self.max_err_to_int = MAXERR_TOINT

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
            i_factor = self.err[0] / self.max_err_to_int
            i_factor = max(0, 1 - i_factor * i_factor) #误差大于MAXERR_TOINT时会停止积分,太接近时会削弱积分
            self.int_i = self.int_i + i_factor * self.ki * (self.err[0] + self.err[1])/2 * dt
            if not D_by_state:
                self.dif_d = (self.err[0] - self.err[1]) / dt
            else:
                self.dif_d = dref - dfdb
        self.t_last = self.t_now

        #积分限幅
        if self.int_i < -self.int_i_MAX:
            self.int_i = -self.int_i_MAX
        elif self.int_i > self.int_i_MAX:
            self.int_i = self.int_i_MAX 

        self.output = self.kp * self.err[0] + self.int_i + self.kd * self.dif_d

        if self.output < -self.output_MAX:
            self.output = -self.output_MAX
        elif self.output > self.output_MAX:
            self.output = self.output_MAX
    
    def vector_cal(self, ref, fdb, D_by_state = False, dref = None, dfdb = None):
        shape = np.shape(ref)
        if self.vec_err is None:
            self.vec_err = [np.zeros(shape), np.zeros(shape)]
            self.vec_int_i = np.zeros(shape)
            self.vec_dif_d = np.zeros(shape)
            self.vec_output = np.zeros(shape)
        self.vec_err[1] = self.vec_err[0].copy()
        self.vec_err[0] = ref - fdb

        self.t_now = rospy.Time.now().to_sec()
        dt = self.t_now - self.t_last
        if dt > 0.2 or dt < 1e-4:
            self.vec_int_i = np.zeros(shape)
            self.vec_dif_d = np.zeros(shape)
        else:
            i_factor = self.vec_err[0] / self.max_err_to_int
            i_factor = np.maximum(0, 1 - i_factor * i_factor)
            self.vec_int_i = self.vec_int_i + i_factor * self.ki * (self.vec_err[0] + self.vec_err[1])/2 * dt
            if not D_by_state:
                self.vec_dif_d = (self.vec_err[0] - self.vec_err[1]) / dt
            else:
                self.vec_dif_d = dref - dfdb
        self.t_last = self.t_now
        self.vec_int_i = np.clip(self.vec_int_i, -self.int_i_MAX, self.int_i_MAX)
        self.vec_output = self.kp * self.vec_err[0] + self.vec_int_i + self.kd * self.vec_dif_d
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
            self.uav_id = '/uav0'
        elif self.role == 'slave':
            self.uav_id = '/' + self.uav_id

        self.traj_xy = np.zeros(2)
        self.traj_z = 0
        self.traj_vel_xy = np.zeros(2)
        self.traj_vel_z = 0
        self.traj_yaw = 0
        self.Follower_xy = np.zeros(2)
        self.Follower_z = 0
        self.Follower_vel_xy = np.zeros(2)
        self.Follower_vel_z = 0
        self.Control_to_vel = Twist()
        self.Control_vel_and_yaw = PositionTarget()
        self.Control_vel_and_yaw.coordinate_frame = PositionTarget.FRAME_LOCAL_NED #注意这里是NED,后续需手动转为ENU
        self.Control_vel_and_yaw.type_mask = (PositionTarget.IGNORE_PX | PositionTarget.IGNORE_PY | PositionTarget.IGNORE_PZ |
                                            PositionTarget.IGNORE_AFX | PositionTarget.IGNORE_AFY | PositionTarget.IGNORE_AFZ |
                                            PositionTarget.IGNORE_YAW_RATE)
        self.traj_ready = False
        self.state_ready = False
        self.yawd_ready = False

        rospy.Subscriber(self.uav_id + '/trajectory', Odometry, self.traj_callback)
        rospy.Subscriber(self.uav_id + '/state', Odometry, self.state_callback)
        rospy.Subscriber(self.uav_id + '/yawd', Float32, self.yawd_callback)
        rospy.Subscriber('/uav0/mavros/state', State, self.Leader_state_callback)
        self.setpoint_velocity_cmd_vel_pub = rospy.Publisher(self.uav_id + '/mavros/setpoint_velocity/cmd_vel_unstamped', Twist, queue_size = 1)
        self.setpoint_velocity_and_yaw_pub = rospy.Publisher(self.uav_id + '/mavros/setpoint_raw/local', PositionTarget, queue_size = 1)

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

    def yawd_callback(self, data):
        self.traj_yaw = data.data
        self.yawd_ready = True

    def Leader_state_callback(self, data):
        self.Leader_mode = data.mode

    def Run(self):
        # PID_horizon = PID_Controller(1.4, 0.0, 0.1, 5.0, 10)
        # PID_height = PID_Controller(0.4, 0.01, 0, 1, 10) #不要差分,第三项一定要置为0
        PID_horizon = PID_Controller(1.4, 0.0, 0.1, output_MAX = 6.0, int_i_MAX = 0.8, MAXERR_TOINT = 0.2)
        PID_height = PID_Controller(0.4, 0.01, 0, output_MAX = 2, int_i_MAX = 0.4, MAXERR_TOINT = 0.5) #不要差分,第三项一定要置为0,测试用限制
        while not rospy.is_shutdown():
            if self.traj_ready and self.state_ready:
                PID_horizon.calculation(self.traj_xy, self.Follower_xy, True, self.traj_vel_xy, self.Follower_vel_xy)
                PID_height.calculation(self.traj_z, self.Follower_z, True, self.traj_vel_z, self.Follower_vel_z)
                if self.role == 'master':
                    if self.Leader_mode != "OFFBOARD":
                        self.Control_to_vel.linear.x = 0.0
                        self.Control_to_vel.linear.y = 0.0
                        self.Control_to_vel.linear.z = 0.0
                    else:
                        self.Control_to_vel.linear.x = PID_horizon.vec_output[0]
                        self.Control_to_vel.linear.y = PID_horizon.vec_output[1]
                        self.Control_to_vel.linear.z = PID_height.output
                    self.setpoint_velocity_cmd_vel_pub.publish(self.Control_to_vel)
                elif self.role == 'slave' and self.yawd_ready: #手动转换坐标系
                    self.Control_vel_and_yaw.velocity.x = PID_horizon.vec_output[0]
                    self.Control_vel_and_yaw.velocity.y = PID_horizon.vec_output[1]
                    self.Control_vel_and_yaw.velocity.z = PID_height.output
                    self.Control_vel_and_yaw.yaw = self.traj_yaw
                    self.setpoint_velocity_and_yaw_pub.publish(self.Control_vel_and_yaw)
            self.rate.sleep()

if __name__ == '__main__':
    try:
        control_loop = Control_loop()
        control_loop.Run()
    except rospy.ROSInterruptException:
        pass