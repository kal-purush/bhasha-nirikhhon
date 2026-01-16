# from __future__ import print_function
import mysql.connector
import sys
import getopt
# sys.path.append('.')
import RTIMU
import os.path
import time
import math
# from AHRS import madgwick
# from AHRS import quaternions
import numpy as np
from datetime import date, datetime, timedelta
import threading


def Welford(count_w, mean_w, M2_w, newSample):
    # (count_w, mean_w, M2_w) = sample
    count_w += 1  # count_n = count_(n-1) + 1
    delta = newSample - mean_w  # x_n - x_mean_(n-1)
    mean_w += delta/count_w  # mean_n = mean_(n-1) + delta/count
    delta2 = newSample - mean_w  # x_n - x_mean_n
    M2_w += delta*delta2  # M2_n = M2_(n-1) + (x_n - x_mean_(n-1))*(x_n - x_mean_n)
    return count_w, mean_w, M2_w


#################################################################################################################
#################################################################################################################

#################################################################################################################
# IMU setup # ###################################################################################################
SETTINGS_FILE = "IMU_config"

print("Using settings file " + SETTINGS_FILE + ".ini")
if not os.path.exists(SETTINGS_FILE + ".ini"):
    print("Settings file does not exist, will be created")
s = RTIMU.Settings(SETTINGS_FILE)
imu = RTIMU.RTIMU(s)
print("IMU Name: " + imu.IMUName())
if not imu.IMUInit():
    print("IMU Init Failed")
    sys.exit(1)
else:
    print("IMU Init Succeeded")
# this is a good time to set any fusion parameters
# imu.setSlerpPower(0.02)
imu.setGyroEnable(True)
imu.setAccelEnable(True)
imu.setCompassEnable(True)
poll_interval = imu.IMUGetPollInterval()
print("Recommended Poll Interval: %dmS\n" % poll_interval)
# AHRS = madgwick.MadgwickAHRS()
#################################################################################################################
#################################################################################################################
start = time.time()
mean = 0
count = 0
M2 = 0
while True:


    imu.IMURead()

    #
    end = time.time()
    # print(end-start)
    if (end-start) > 1:
        start = time.time()
        # print(imu.getAccel(), "||", imu.getGyro())
    (count, mean, M2) = Welford(count, mean, M2, imu.getAccel()[2])

    if count % 1000 == 0 and count != 0:
        print(mean, "|", count, "|", M2)
        print(np.sqrt(M2/count))



    # duration = end - start
    # if duration < poll_interval:
    #     time.sleep(poll_interval - duration)