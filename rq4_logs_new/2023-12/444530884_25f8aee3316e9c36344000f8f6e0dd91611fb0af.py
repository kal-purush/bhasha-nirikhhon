#!/usr/bin/env python3

# Copyright (c) 2019 Computer Vision Center (CVC) at the Universitat Autonoma de
# Barcelona (UAB).
#
# This work is licensed under the terms of the MIT license.
# For a copy, see <https://opensource.org/licenses/MIT>.

"""Spawn NPCs into the simulation"""

import glob
import os
import sys
import time
import numpy as np

from find_carla_egg import find_carla_egg

carla_egg_file = find_carla_egg()

sys.path.append(carla_egg_file)

import carla

import argparse
import logging
from numpy import random
from pynput import keyboard
from pynput.keyboard import Key

def rotate_3d_vector_90(orig_vector,axis):
    # Convert degrees to radians
    theta = np.radians(90)
    if axis == "x":
        rotation_vector = [1, 0, 0]
    if axis == "y":
        rotation_vector = [0, 1, 0]
    if axis == "z":
        rotation_vector = [0, 0, 1]


    # Define the 3D rotation matrix around the z-axis
    rotation_matrix = np.array([[np.cos(theta), -np.sin(theta), 0],
                                [np.sin(theta), np.cos(theta), 0],
                                rotation_vector])

    # Apply the rotation matrix to the vector
    rotated_vector = np.dot(rotation_matrix, orig_vector)

    return rotated_vector

def on_release(key):

    world = client.get_world()
        
    # Retrieve the spectator object
    spectator = world.get_spectator()

    # Get the location and rotation of the spectator through its transform
    spec_transform = spectator.get_transform()

    try:
        key_char = key.char
    except Exception:
        key_char = None

    if key == Key.esc:
        return False
    elif key == Key.right:
        # print("Right key clicked")
        spec_location = spec_transform.location + carla.Location(x=0, y=0, z=0)
        spec_rotation = carla.Rotation(  pitch=spec_transform.rotation.pitch + 0,
                                            yaw=spec_transform.rotation.yaw + rotate_step,
                                            roll=spec_transform.rotation.roll )
    elif key == Key.left:
        # print("Left key clicked")
        spec_location = spec_transform.location + carla.Location(x=0, y=0, z=0)
        spec_rotation = carla.Rotation(  pitch=spec_transform.rotation.pitch + 0,
                                            yaw=spec_transform.rotation.yaw - rotate_step,
                                            roll=spec_transform.rotation.roll )
        
    elif key == Key.up:
        # print("Up key clicked")
        spec_location = spec_transform.location + carla.Location(x=0, y=0, z=0)
        spec_rotation = carla.Rotation(  pitch=spec_transform.rotation.pitch + rotate_step,
                                            yaw=spec_transform.rotation.yaw + 0,
                                            roll=spec_transform.rotation.roll )
        
    elif key == Key.down:
        # print("Down key clicked")
        spec_location = spec_transform.location + carla.Location(x=0, y=0, z=0)
        spec_rotation = carla.Rotation(  pitch=spec_transform.rotation.pitch - rotate_step,
                                            yaw=spec_transform.rotation.yaw + 0,
                                            roll=spec_transform.rotation.roll )
        
    elif key_char == "e":
        # print("E key clicked")
        
        # spec_forward_vector = spec_transform.get_forward_vector()
        # print("spec_forward_vector: " + str(spec_forward_vector))
        # spec_up_vector = rotate_3d_vector_90([spec_forward_vector.x,spec_forward_vector.y,spec_forward_vector.z],"y")
        # spec_up_vector = carla.Location(x=spec_up_vector[0],y=spec_up_vector[1],z=spec_up_vector[2])
        # print("spec_up_vector: " + str(spec_up_vector))
        # spec_location = spec_transform.location + spec_up_vector*move_step
        
        
        spec_location = spec_transform.location + carla.Location(x=0, y=0, z=move_step)
        spec_rotation = carla.Rotation(  pitch=spec_transform.rotation.pitch + 0,
                                            yaw=spec_transform.rotation.yaw + 0,
                                            roll=spec_transform.rotation.roll )
    elif key_char == "q":
        # print("Q key clicked")
        spec_location = spec_transform.location + carla.Location(x=0, y=0, z=-move_step)
        spec_rotation = carla.Rotation(  pitch=spec_transform.rotation.pitch + 0,
                                            yaw=spec_transform.rotation.yaw + 0,
                                            roll=spec_transform.rotation.roll )
    elif key_char == "w":
        # print("W key clicked")
        spec_location = spec_transform.location + spec_transform.get_forward_vector()*move_step
        spec_rotation = carla.Rotation(  pitch=spec_transform.rotation.pitch + 0,
                                            yaw=spec_transform.rotation.yaw + 0,
                                            roll=spec_transform.rotation.roll )
    elif key_char == "s":
        # print("S key clicked")
        spec_location = spec_transform.location - spec_transform.get_forward_vector()*move_step
        spec_rotation = carla.Rotation(  pitch=spec_transform.rotation.pitch + 0,
                                            yaw=spec_transform.rotation.yaw + 0,
                                            roll=spec_transform.rotation.roll )
        
    elif key_char == "a":
        # print("A key clicked")
        spec_forward_vector = spec_transform.get_forward_vector()
        spec_left_vector = carla.Location( x=spec_forward_vector.y, 
                                            y=-1*spec_forward_vector.x,
                                            z=0 )
        spec_location = spec_transform.location + spec_left_vector*move_step
        spec_rotation = carla.Rotation(  pitch=spec_transform.rotation.pitch + 0,
                                            yaw=spec_transform.rotation.yaw + 0,
                                            roll=spec_transform.rotation.roll )
    elif key_char == "d":
        # print("D key clicked")
        spec_forward_vector = spec_transform.get_forward_vector()
        spec_right_vector = carla.Location( x=-1*spec_forward_vector.y, 
                                            y=spec_forward_vector.x,
                                            z=0 )
        spec_location = spec_transform.location + spec_right_vector*move_step
        spec_rotation = carla.Rotation(  pitch=spec_transform.rotation.pitch + 0,
                                            yaw=spec_transform.rotation.yaw + 0,
                                            roll=spec_transform.rotation.roll )
    else:
        return
    
        
    

    spectator.set_transform(carla.Transform(spec_location,spec_rotation))

def test_on_press(key):
    try:
        print('alphanumeric key {0} pressed'.format(
            key.char))
    except AttributeError:
        print('special key {0} pressed'.format(
            key))
    
def test_on_release(key):
    print('{0} released'.format(
        key))
    if key == keyboard.Key.esc:
        # Stop listener
        return False

# def main():
argparser = argparse.ArgumentParser(
    description=__doc__)
argparser.add_argument(
    '--host',
    metavar='H',
    default='127.0.0.1',
    help='IP of the host server (default: 127.0.0.1)')
argparser.add_argument(
    '-p', '--port',
    metavar='P',
    default=2000,
    type=int,
    help='TCP port to listen to (default: 2000)')
args = argparser.parse_args()


client = carla.Client(args.host, args.port)
client.set_timeout(10.0)

move_step = 10
rotate_step = 10

# listener = keyboard.Listener(on_release=on_release)
# listener.start()



    # while True:
try:
    with keyboard.Listener(
        # on_press=on_press,
        on_release=on_release) as listener:
        listener.join()
    # world = client.get_world()
    
    # # Retrieve the spectator object
    # spectator = world.get_spectator()

    # # Get the location and rotation of the spectator through its transform
    # spec_transform = spectator.get_transform()

    # # print(str(spec_transform))
    # # spec_location = spec_transform.location + carla.Location(x=move_step, y=0, z=0)
    # # spec_rotation = spec_transform.rotation + carla.Rotation(pitch=0, yaw=0, roll=0)

    # # # Set the spectator with an empty transform
    # spectator.set_transform(carla.Transform(spec_location,spec_rotation))

finally:
    print('\n----- STOPPING SPECTATOR CONTROL -----\n')
    time.sleep(0.5)

# if __name__ == '__main__':

#     try:
#         main()
#     except KeyboardInterrupt:
#         pass