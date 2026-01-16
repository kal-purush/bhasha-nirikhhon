from launch import LaunchDescription
from launch_ros.actions import Node

def generate_launch_description():
    ld = LaunchDescription()

    camera_node = Node(
        package="team52",
        executable="camera",
    )
    ld.add_action(camera_node)

    allies_detection_node = Node(
        package="team52",
        executable="allies_detection",
    )
    ld.add_action(allies_detection_node)
    
    lidar_node = Node(
        package="team52",
        executable="lidar_converter",
    )
    ld.add_action(lidar_node)
    
    pid_node = Node(
        package="team52",
        executable="pid",
    )
    ld.add_action(pid_node)
    
    beacon_node = Node(
        package="team52",
        executable="beacon_converter",
    )
    ld.add_action(beacon_node)
    
    brain_node = Node(
        package="team52",
        executable="brain",
    )
    ld.add_action(brain_node)
    
    gps_node = Node(
        package="team52",
        executable="gps_converter",
    )
    ld.add_action(gps_node)
    
    pathfinder_node = Node(
        package="team52",
        executable="pathfinder",
    )
    ld.add_action(pathfinder_node)

    return ld