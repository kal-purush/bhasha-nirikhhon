import rclpy
from rclpy.node import Node

from sensor_msgs.msg import NavSatFix
from team52_interfaces.msg import Obstacles

class PathFinder(Node):

    def __init__(self):
        super().__init__("path_finder")

        # > Subscription < #
        self.beacon_subs = self.create_subscription(
            Obstacles,
            '/team52/boat_obstacles',
            self.get_beacon,
            10
        )

        # > Publisher < #
        self.waypont_pubs = self.create_publisher(
            NavSatFix,
            '/team52/waypoint',
            10
        )

    def get_beacon(self, obstacle):
        self.waypont_pubs.publish(obstacle.gps_list[2])


def main():
    rclpy.init()

    pathfinder = PathFinder()
    rclpy.spin(pathfinder)

    rclpy.shutdown()

if __name__ == "__main__":
    main()