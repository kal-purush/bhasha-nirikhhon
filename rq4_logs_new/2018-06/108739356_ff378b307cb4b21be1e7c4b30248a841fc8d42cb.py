#
# Original work Copyright (c) 2014 Danny Zhu
# Modified work Copyright (c) 2018 Matthias Gazzari
#
# Licensed under the MIT license. See the LICENSE file for details.
#

import argparse
from myo_raw import MyoRaw


def emg_handler(emg, moving):
    print('emg:', emg, moving)

def imu_handler(quat, acc, gyro,):
    print('imu:', quat, acc, gyro)

def battery_handler(battery_level):
    print('battery level:', battery_level)


parser = argparse.ArgumentParser()
parser.add_argument('--tty', default=None, help='The Myo dongle device (autodetected if omitted)')
parser.add_argument('--mac', default=None, help='The Myo MAC address (arbitrarily detected if omitted)')
parser.add_argument('--filtered', default=False, action='store_true', help='Get filtered EMG data')
args = parser.parse_args()

# setup the Myo dongle
myo = MyoRaw(args.tty)
# add handlers to process EMG, IMU and battery level data
myo.add_emg_handler(emg_handler)
myo.add_imu_handler(imu_handler)
myo.add_battery_handler(battery_handler)
# connect to a Myo device and set whether the EMG data shall be filtered or not
myo.connect(args.mac, args.filtered)
# disable sleep to avoid disconnects while retrieving data
myo.sleep_mode(1)
# vibrate and change colors (green logo, blue bar) to signalise a successfull setup
myo.vibrate(1)
myo.set_leds([0, 255, 0], [0, 0, 255])

# run until terminated by the user
try:
    while True:
        myo.run(1)
except KeyboardInterrupt:
    pass
finally:
    myo.disconnect()
    print('Disconnected')