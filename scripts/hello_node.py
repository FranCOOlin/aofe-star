#!/usr/bin/env python3

import rospy
from std_msgs.msg import String

def main():
    rospy.init_node("hello_node")

    pub = rospy.Publisher("hello_topic", String, queue_size=10)

    rate = rospy.Rate(1)

    count = 0
    while not rospy.is_shutdown():
        msg = "hello rospy, count = {}".format(count)
        rospy.loginfo(msg)
        pub.publish(msg)

        count += 1
        rate.sleep()

if __name__ == "__main__":
    main()
