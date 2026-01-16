import rclpy
from rclpy.node import Node

import math

from sensor_msgs.msg import Imu
from sensor_msgs.msg import NavSatFix
from ros_gz_interfaces.msg import ParamVec
from team52_interfaces.srv import BeaconToGPS


#>===[ QUATERNION TO YAW ]===<#
def quaternion_to_yaw(qw, qx, qy, qz):
    return math.atan2(2 * (qw * qz + qx * qy), 1 - 2 * (qy**2 + qz**2))


#>===[ SUB NODE ]===<#
class GPSConverter(Node):

    # --- Initialization --- #
    def __init__(self):
        super().__init__('beacon_gps_client')
        # Beacon To GPS #
        self.cli_btogps = self.create_client(BeaconToGPS, '/team52/beacon_to_gps')
        while not self.cli_btogps.wait_for_service(timeout_sec=1.0):
            self.get_logger().info('service not available, waiting again...')
        self.request_beacon = BeaconToGPS.Request()

    def send_request(self, distance, angle):
        self.request_beacon.distance = distance
        self.request_beacon.angle = angle
        self.future_gps = self.cli_btogps.call_async(self.request_beacon)
        rclpy.spin_until_future_complete(self, self.future_gps)
        return self.future_gps.result().gps
    

class Converter(Node):

    def __init__(self):
        super().__init__('beacon_converter')
        self.yaw = 0
        self.subs_tilts = self.create_subscription(
            Imu,
            '/wamv/sensors/imu/imu/data',
            self.orientation,
            10
        )
        self.beacon_subscriber = self.create_subscription(
            ParamVec,
            '/wamv/sensors/acoustics/receiver/range_bearing',
            self.get_beacon_gps,
            10
        )
        self.beacon_publisher = self.create_publisher(
            NavSatFix,
            '/team52/beacon',
            10
        )

        # Service #
        self.client_gps_converter = GPSConverter()
        self.get_logger().info("Node ready")

    # --- Correct the 2D position --- #
    def horizontal_correction(self, point):
        # Radius & Angle #
        radius = math.sqrt(point[0]**2+point[1]**2)
        angle = math.atan2(point[1], point[0])
        # New point #
        point[0] = radius*math.cos(angle+self.yaw)
        point[1] = radius*math.sin(angle+self.yaw)
        self.get_logger().info("Node ready")
    
    # --- Set tilts --- #
    def orientation(self, data: Imu):
        w = data._orientation.w
        x = data._orientation.x
        y = data._orientation.y
        z = data._orientation.z
        self.yaw = quaternion_to_yaw(w,x,y,z)
    
    def get_beacon_gps(self, data):
        for category in data.params:
            if category.name == 'range':
                distance = category.value.double_value
            if category.name == 'bearing':
                angle = category.value.double_value
        beacon_gps = self.client_gps_converter.send_request(distance, angle+self.yaw)
        self.beacon_publisher.publish(beacon_gps)


def main() :
    rclpy.init()
    beacon_converter = Converter()

    rclpy.spin(beacon_converter)
    
    rclpy.shutdown()

if __name__ == '__main__':
    main()