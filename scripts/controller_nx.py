#!/usr/bin/python3
#coding=utf-8
import rospy
from nav_msgs.msg import Odometry
from std_msgs.msg import Float32
from geometry_msgs.msg import Twist
from mavros_msgs.msg import PositionTarget, State
import numpy as np

def get_bool_param(name, default=False):
    """
    读取 ROS 参数中的 bool 开关。

    roslaunch 里写入的布尔值有时会以字符串形式进入参数服务器；
    直接 bool("false") 会得到 True，所以这里显式解析常见真值字符串。
    """
    value = rospy.get_param(name, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ['true', '1', 'yes', 'on']
    return bool(value)

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

        # ============================================================
        # Master velocity feedforward 参数。
        #
        # 设计约定：
        #   1. 只在 slave 控制器中使用；
        #   2. 前馈源是 master MAVROS setpoint_raw/target_local；
        #   3. MAVROS 发布到 ROS topic 后，velocity 按 ROS ENU 语义表达；
        #   4. 前馈默认关闭，实飞前先确认 target_local 中 velocity 有效。
        # ============================================================
        self.enable_master_velocity_feedforward = get_bool_param(
            '~enable_master_velocity_feedforward',
            False,
        )
        self.master_velocity_feedforward_topic = rospy.get_param(
            '~master_velocity_feedforward_topic',
            '/uav0/mavros/setpoint_raw/target_local',
        )
        self.master_velocity_feedforward_timeout = float(
            rospy.get_param('~master_velocity_feedforward_timeout', 0.25)
        )
        self.master_velocity_feedforward_gain_xy = float(
            rospy.get_param('~master_velocity_feedforward_gain_xy', 1.0)
        )
        self.master_velocity_feedforward_gain_z = float(
            rospy.get_param('~master_velocity_feedforward_gain_z', 0.0)
        )

        # 最终限幅只在实际叠加前馈时启用；无前馈/超时/ignore 时保持原 PID 输出。
        self.slave_cmd_vel_limit_xy = float(
            rospy.get_param('~slave_cmd_vel_limit_xy', 6.0)
        )
        self.slave_cmd_vel_limit_z = float(
            rospy.get_param('~slave_cmd_vel_limit_z', 2.0)
        )

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

        # 缓存最近一次 master 前馈速度。
        # master_ff_last_recv_time 为 None 表示还没有收到过有效 topic 回调。
        self.master_ff_vel_xy = np.zeros(2)
        self.master_ff_vel_z = 0.0
        self.master_ff_last_recv_time = None

        rospy.Subscriber(self.uav_id + '/trajectory', Odometry, self.traj_callback)
        rospy.Subscriber(self.uav_id + '/state', Odometry, self.state_callback)
        rospy.Subscriber(self.uav_id + '/yawd', Float32, self.yawd_callback)
        rospy.Subscriber('/uav0/mavros/state', State, self.Leader_state_callback)

        # 只有 slave 且显式打开开关时才订阅 master 前馈，避免默认行为变化。
        if self.role == 'slave' and self.enable_master_velocity_feedforward:
            rospy.Subscriber(
                self.master_velocity_feedforward_topic,
                PositionTarget,
                self.master_velocity_feedforward_callback,
            )
            rospy.logwarn(
                "[%s Controller] master velocity feedforward enabled: topic=%s timeout=%.3fs gain_xy=%.3f gain_z=%.3f",
                self.uav_id,
                self.master_velocity_feedforward_topic,
                self.master_velocity_feedforward_timeout,
                self.master_velocity_feedforward_gain_xy,
                self.master_velocity_feedforward_gain_z,
            )
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

    def master_velocity_feedforward_callback(self, data):
        """
        缓存 master 的 PX4 期望速度前馈。

        data 来自 /uav0/mavros/setpoint_raw/target_local，类型为 PositionTarget。
        PX4/MAVLink 原始消息是 POSITION_TARGET_LOCAL_NED；MAVROS 在发布 ROS topic
        时已经把 position/velocity/acceleration 转为 ENU 数值。这里不再根据
        coordinate_frame 做 ENU/NED 转换，只按 type_mask 判断各轴 velocity 是否有效。
        """
        # POSITION_TARGET_TYPEMASK 中 IGNORE_V* 置位代表该轴速度目标无效。
        # 无效或非有限数的轴不参与前馈，直接置 0。
        ff_x_valid = (
            not bool(data.type_mask & PositionTarget.IGNORE_VX)
            and np.isfinite(data.velocity.x)
        )
        ff_y_valid = (
            not bool(data.type_mask & PositionTarget.IGNORE_VY)
            and np.isfinite(data.velocity.y)
        )
        ff_z_valid = (
            not bool(data.type_mask & PositionTarget.IGNORE_VZ)
            and np.isfinite(data.velocity.z)
        )

        # 按轴缓存速度前馈，允许 x/y/z 部分有效。
        self.master_ff_vel_xy = np.array([
            data.velocity.x if ff_x_valid else 0.0,
            data.velocity.y if ff_y_valid else 0.0,
        ])
        self.master_ff_vel_z = data.velocity.z if ff_z_valid else 0.0
        self.master_ff_last_recv_time = rospy.Time.now().to_sec()

    def get_master_velocity_feedforward(self):
        """
        返回当前可用的 master 速度前馈。

        返回值始终是 ROS ENU 语义的 (xy, z)。当前馈开关关闭、尚未收到消息、
        或消息超时时，返回 0，使 slave 完全退回原 PID 控制。
        """
        if not self.enable_master_velocity_feedforward:
            return np.zeros(2), 0.0

        if self.master_ff_last_recv_time is None:
            return np.zeros(2), 0.0

        if rospy.Time.now().to_sec() - self.master_ff_last_recv_time > self.master_velocity_feedforward_timeout:
            return np.zeros(2), 0.0

        return self.master_ff_vel_xy.copy(), self.master_ff_vel_z

    def limit_horizontal_velocity(self, vel_xy):
        """
        对水平速度做向量模长限幅，避免 x/y 分量分别限幅后改变方向。
        """
        if self.slave_cmd_vel_limit_xy <= 0.0:
            return np.zeros(2)

        norm = np.linalg.norm(vel_xy)
        if norm > self.slave_cmd_vel_limit_xy:
            return vel_xy / norm * self.slave_cmd_vel_limit_xy

        return vel_xy

    def limit_vertical_velocity(self, vel_z):
        """
        对垂向速度做对称限幅；z 仍按本控制器内部 ROS ENU 语义处理。
        """
        if self.slave_cmd_vel_limit_z <= 0.0:
            return 0.0

        return float(np.clip(vel_z, -self.slave_cmd_vel_limit_z, self.slave_cmd_vel_limit_z))

    def Run(self):
        # PID_horizon = PID_Controller(1.4, 0.0, 0.1, 5.0, 10)
        # PID_height = PID_Controller(0.4, 0.01, 0, 1, 10) #不要差分,第三项一定要置为0
        PID_horizon = PID_Controller(1.6, 0.0, 0.2, output_MAX = 6.0, int_i_MAX = 0.8, MAXERR_TOINT = 0.2)
        PID_height = PID_Controller(1, 0.01, 0, output_MAX = 2, int_i_MAX = 0.4, MAXERR_TOINT = 0.5) #不要差分,第三项一定要置为0,测试用限制
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
                    # slave 原始输出是位置/速度误差 PID；前馈是 master PX4 在 POSCTL/POSITION
                    # 下给出的期望速度。两者都按本控制器内部的 local ENU 数值叠加。
                    ff_vel_xy, ff_vel_z = self.get_master_velocity_feedforward()
                    ff_cmd_xy = self.master_velocity_feedforward_gain_xy * ff_vel_xy
                    ff_cmd_z = self.master_velocity_feedforward_gain_z * ff_vel_z
                    cmd_vel_xy = PID_horizon.vec_output + ff_cmd_xy
                    cmd_vel_z = PID_height.output + ff_cmd_z

                    # 只有实际叠加了非零前馈时才做最终限幅。
                    # 这样前馈关闭、topic 超时、或 velocity 被 ignore 时，输出严格保持原 PID 行为。
                    if np.any(np.abs(ff_cmd_xy) > 1e-9):
                        cmd_vel_xy = self.limit_horizontal_velocity(cmd_vel_xy)
                    if abs(ff_cmd_z) > 1e-9:
                        cmd_vel_z = self.limit_vertical_velocity(cmd_vel_z)

                    self.Control_vel_and_yaw.velocity.x = cmd_vel_xy[0]
                    self.Control_vel_and_yaw.velocity.y = cmd_vel_xy[1]
                    self.Control_vel_and_yaw.velocity.z = cmd_vel_z
                    self.Control_vel_and_yaw.yaw = self.traj_yaw
                    self.setpoint_velocity_and_yaw_pub.publish(self.Control_vel_and_yaw)
            self.rate.sleep()

if __name__ == '__main__':
    try:
        control_loop = Control_loop()
        control_loop.Run()
    except rospy.ROSInterruptException:
        pass
