import rclpy
from rclpy.node import Node

from std_msgs.msg import UInt32
from geometry_msgs.msg import PoseStamped
from sensor_msgs.msg import NavSatFix

class Brain(Node):

    def __init__(self):
        super().__init__("brain")

        # > Subscribers < #
        self.phase = 0
        self.phase_subs = self.create_subscription(
            UInt32,
            '/vrx/patrolandfollow/current_phase',
            self.process_phase,
            10
        )
        self.beacon_subs = self.create_subscription(
            NavSatFix,
            "/team52/beacon",
            self.get_beacon,
            10
        )
        self.lidar_enemy_subs = self.create_subscription(
            NavSatFix,
            "/team52/lidar_enemy",
            self.get_lidar_enemy,
            10
        )

        # > Publishers < #
        self.objective_pubs = self.create_publisher(
            NavSatFix,
            '/team52/objective',
            10
        )
        self.alert_pubs = self.create_publisher(
            PoseStamped,
            '/vrx/patrolandfollow/alert_position',
            10
        )

    def get_lidar_enemy(self, gps: NavSatFix):
        if self.phase == 2:
            position = PoseStamped()
            position.pose.position.y = gps.longitude
            position.pose.position.x = gps.latitude
            position.pose.position.z = gps.altitude
            self.alert_pubs.publish(position)
        if self.phase == 3:
            self.objective_pubs.publish(gps)

    def get_beacon(self, gps):
        if self.phase == 1:
            self.objective_pubs.publish(gps)

    def process_phase(self, phase):
        self.phase = phase.data


def main():
    rclpy.init()

    brain = Brain()
    rclpy.spin(brain)

    rclpy.shutdown()

if __name__ == "__main__":
    main()