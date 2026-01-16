"""Docstring to be filled
With stuff written
"""

import numpy as np


class PoseEstimator(object):
    """stuff
    ddd
    """
    STEERING_WHEEL_RADIUS = 0.2
    ENCODER_RESOLUTION = 512
    TURNING_RADIUS = 1

    def __init__(self, sensor_weight = 0.5):
        self.sensor_weight = sensor_weight
        self.last_pose = (0, 0, 0)
        self.last_time = 0
        self.last_tick = 0

    '''estimate (time, steering_angle, encoder_ticks, angular_velocity) returns
    estimated_pose (x, y, heading)
    '''
    def estimate(self, time, steering_angle, encoder_ticks, angular_velocity_imu):
        delta_t = time - self.last_time
        delta_ticks = encoder_ticks - self.last_tick
        v_steering_encoder = 2 * np.pi * STEERING_WHEEL_RADIUS / ENCODER_RESOLUTION * delta_ticks / delta_t
        angular_velocity_encoder = v_steering_encoder * np.sin(steering_angle)
        if (steering_angle != 0):
            v_steering_imu = angular_velocity_imu * TURNING_RADIUS / np.sin(steering_angle)
            v_steering = sensor_fusion(v_steering_encoder, v_steering_imu)
            angular_velocity = sensor_fusion(angular_velocity_encoder, angular_velocity_imu)
        else:
            v_steering = v_steering_encoder
            angular_velocity = angular_velocity_encoder
        delta_heading = angular_velocity * delta_t
        delta_x = v_steering * np.cos(steering_angle) * np.cos(heading) * delta_t
        delta_y = v_steering * np.cos(steering_angle) * np.sin(heading) * delta_t
        self.last_time = time
        self.last_tick = encoder_ticks
        self.last_pose = tuple(map(sum, zip(self.last_pose, (delta_x, delta_y, delta_heading))))
        return self.last_pose

    '''estimate (time, steering_angle, encoder_ticks, angular_velocity) returns
    estimated_pose (x, y, heading)
    '''
    def sensor_fusion(self, encoder_value, imu_value):
        return encoder_value * (self.sensor_weight) + imu_value * (1-self.sensor_weight)