import socket, cv2
import time
import imagezmq
import libcamera
from picamera2 import Picamera2
from picamera2.encoders import H264Encoder
from picamera2.outputs import FfmpegOutput
from settings import picam_config, picam_controls
from processing import proc_frame
import RPi.GPIO as GPIO

GPIO.setmode(GPIO.BOARD)
GPIO.setup(40, GPIO.OUT)

# 0 is nightvision, 1 is daytime
GPIO.output(40, 1)

# camera setup
picam2 = Picamera2()
cam_config = picam_config(picam2)
video_config = picam2.create_video_configuration(**cam_config)
picam2.configure(video_config)
picam_controls(picam2)

encoder = H264Encoder(10000000)
output = FfmpegOutput('test.mp4')
picam2.start_recording(encoder, output)
sender = imagezmq.ImageSender(connect_to='tcp://192.168.0.226:5555')
rpi_name = socket.gethostname() # send RPi hostname with each image

# set prev to None 
prev = None

while True:  # send images as stream until Ctrl-C
    frame = picam2.capture_array("lores")

    ## do something with image
    new_prev, img = proc_frame(prev, frame)
    # print(picam2.capture_metadata())
    sender.send_image(rpi_name, img)

    # # Finally, set the prev to a new value
    prev = new_prev