
import sys
import time

#
# STILL request function 
#
def process_still_req(message):

   cam_message_list = message.split(',')
   print(cam_message_list)
   message = "SENSOR_REP,DEV=PI_CAMERA,SUB_DEV=CAM,CMD=CAPTURE_PIC,SENSOR_REP_END"

   return message

#
# VID request function 
#
def process_video_req(message):
   cam_message_list = message.split(',')
   print(cam_message_list)
   message = "SENSOR_REP,DEV=PI_CAMERA,SUB_DEV=CAM,CMD=CAPTURE_VIDEO,SENSOR_REP_END"

   return message

#
# High level sensor request function
#
def process_sensor_req(message):
   # Depending on the ID, pass it to the needed sensor function
   message_list = message.split(',')   

   # This is where the right server function is called 
   if message_list[3] == 'CMD=CAPTURE_STILL':
      process_still_req(message) 
   elif message_list[3] == 'CMD=CAPTURE_VIDEO':
      process_video_req(message) 
   else:
      # unknown message
      message = "SENSOR_REP," + message_list[1] + ",ERROR=UNKNOWN_ID,SENSOR_REP_END"

   return message
