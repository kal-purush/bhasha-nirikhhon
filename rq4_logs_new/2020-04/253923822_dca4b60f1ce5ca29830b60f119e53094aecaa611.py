import sys
from ambf_client import Client
from Controller import PDController
import numpy as np
import time
from Model.Human import Human
import ambf_msgs.msg as ambf


# Create Client and connect
_client = Client()
_client.connect()
print(_client.get_obj_names())

# body_handle = _client.get_obj_handle('body')
h = Human(_client, 50, 1.5)
q_goal = [0.0] * 7
qd_goal = [0.0] * 7
qdd_goal = [0.0] * 7


def go(count):

    q = h.q
    qd = h.qd

    for i in range(6):
        q_goal[i] = 0
        qd_goal[i] = 0
        qdd_goal[i] = 0

    aq = qdd_goal + Controller.calc(q_goal - q, qd_goal - qd)
    tau = h.calculate_dynamics(q_d  , qd_d, aq)

    for i in range(6):
        cmd[i] = tau[i + 1]

    h.send_command(cmd)

    time.sleep(0.01)

def send_command(self, tau):
    cmd = ambf.ObjectCmd()
    cmd.publish_joint_names = True
    cmd.publish_joint_positions = True
    cmd.enable_position_controller = False
    cmd.joint_cmds = tau
    self.tau_pub.publish(cmd)
