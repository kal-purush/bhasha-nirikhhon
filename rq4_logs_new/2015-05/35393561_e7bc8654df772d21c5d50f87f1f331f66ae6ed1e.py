# Settings Module
import os
# import serial

# os.chdir('/mnt/NAS/code')													
# ser = serial.Serial('/dev/ttyAMA0', 19200, timeout=1)

url = 'http://ec2-54-243-218-204.compute-1.amazonaws.com/index.php/api/RegisterOrbdevice/getOrbMessageDevice'

payload = {"data": {"device_id": "7924"}}