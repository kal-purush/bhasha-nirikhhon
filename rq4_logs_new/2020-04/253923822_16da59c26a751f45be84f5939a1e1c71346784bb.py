# Import the Client from ambf_client package
from ambf_client import Client
from Utlities import Plotter
import rospy
import numpy as np
from std_msgs.msg import Float32MultiArray
from Model import Exoskeleton
from Controller import DynController



def get_traj(q0, qf, v0, vf, tf, dt):

    b = np.array([q0, v0, qf, vf]).reshape((-1,1))
    A = np.array([[1.0, 0.0, 0.0, 0.0],
                  [0.0, 1.0, 0.0, 0.0],
                  [1.0, 0.0, tf ** 2, tf ** 3],
                  [0.0, 0.0, 2 * tf, 3 * tf * 2]])

    x = np.linalg.solve(A, b)
    q = []
    qd = []
    qdd = []

    for t in np.linspace(0, tf, tf/dt):
        q.append(x[0] + x[1] * t + x[2] * t * t + x[3] * t * t * t)
        qd.append(x[1] + 2*x[2] * t + 3*x[3] * t * t)
        qdd.append(2*x[2] + 6*x[3] * t)

    return q, qd, qdd

if __name__ == "__main__":
    pub = rospy.Publisher('qd',Float32MultiArray, queue_size=1)
    pub_goal = rospy.Publisher('goal', Float32MultiArray, queue_size=1)
    msg_vel = Float32MultiArray()
    msg_goal = Float32MultiArray()


    _client = Client()
    _client.connect()
    rate = rospy.Rate(1000)
    LARRE = Exoskeleton.Exoskeleton(_client, 56, 1.56)
    leg_plot = Plotter.Plotter(LARRE)

    Kp = np.zeros((6, 6))
    Kd = np.zeros((6, 6))
    Kp[0,0] = 0.0
    Kd[0, 0] = 0.0
    Kp[1, 1] = 135.0
    Kd[1, 1] = 1.5
    Kp[2, 2] = 110.0
    Kd[2, 2] = 0.5

    crl = DynController.DynController(LARRE, Kp, Kd)
    dt = 0.1
    tf = 2.0
    q_hip, qd_hip, qdd_hip = get_traj(0.0, -0.5, 0.0, 0.0, tf, dt)
    q_knee, qd_knee, qdd_knee = get_traj(0.0, 1.15, 0.0, 0.0, tf, dt)
    q_ankle, qd_ankle, qdd_ankle = get_traj(-0.349, 0, 0.0, 0.0, tf, dt)
    count = 0

    while not rospy.is_shutdown():
        count = min(count, int(tf/dt)-1)

        q_d = np.array([q_hip[count].item(), q_knee[count].item(), q_ankle[count].item(), 0.0, 0.0, 0.0])
        qd_d = np.array([qd_hip[count].item(), qd_knee[count].item(), qd_ankle[count].item(), 0.0, 0.0, 0.0])
        qdd_d = np.array([qdd_hip[count].item(), qdd_knee[count].item(), qdd_ankle[count].item(), 0.0, 0.0, 0.0])
        crl.calc_tau(q_d, qd_d)
        msg_vel.data = LARRE.q
        msg_goal.data = q_d
        leg_plot.update()
        pub.publish(msg_vel)
        pub_goal.publish(msg_goal)
        count += 1
        rate.sleep()

    _client.clean_up()




