#!/usr/bin/env python3
# encoding: utf-8


def get_camera_options():
    return {
        'resolution': (1024, 768),
        'framerate': 1,
        'sensor_mode': 3,
        # TODO ? led_pin = None
    }


def set_to_night_vision(cam):
    # set just once cam.resolution = (1024, 768)
    cam.brightness = 90
    cam.exposure_mode = 'night'
    cam.framerate = 1  # set framerate to 1fps
    cam.iso = 1600
    cam.sensor_mode = 3  # force long exposure mode
    cam.shutter_speed = 1000000  # set shutter speed to 1s

    # TODO?: cam.flash_mode = 'on'
    # TODO?: IR diodes


def reset(cam):
    cam.brightness = 50
    cam.exposure_mode = 'auto'
    cam.framerate = 30
    cam.iso = 0
    cam.sensor_mode = 3
    cam.shutter_speed = 0


def close(cam):
    pass