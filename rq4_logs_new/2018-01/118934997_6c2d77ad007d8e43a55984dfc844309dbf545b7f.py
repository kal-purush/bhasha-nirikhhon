import numpy as np 
import cv2
import serial
from picamera.array import PiRGBArray
from picamera import PiCamera
import time
import argparse
import imutils
from threading import *
from rplidar import RPLidar
upperbody_cascade = cv2.CascadeClassifier('haarcascade_upperbody.xml')

class PiVideoStream:
	def __init__(self, resolution=(192, 112), framerate=32):
		# initialize the camera and stream
		self.camera = PiCamera()
		self.camera.resolution = resolution
		self.camera.rotation = 180
		self.camera.framerate = framerate
		self.rawCapture = PiRGBArray(self.camera, size=resolution)
		self.stream = self.camera.capture_continuous(self.rawCapture,format="bgr", use_video_port=True)
		# initialize the frame and the variable used to indicate
		# if the thread should be stopped
		self.frame = None
		self.stopped = False

	def start(self):
		# start the thread to read frames from the video stream
		Thread(target=self.update, args=()).start()
		return self

	def update(self):
		# keep looping infinitely until the thread is stopped
		for f in self.stream:
			# grab the frame from the stream and clear the stream in
			# preparation for the next frame
			self.frame = f.array
			self.rawCapture.truncate(0)

			# if the thread indicator variable is set, stop the thread
			# and resource camera resources
			if self.stopped:
				self.stream.close()
				self.rawCapture.close()
				self.camera.close()
				return

	def read(self):
		return self.frame

	def stop(self):
		self.stopped = True

cv2.setUseOptimized(True)
resheight = 0
vs = PiVideoStream().start()
arduino = serial.Serial("/dev/ttyACM1", 9600, timeout=.1)
time.sleep(0.5)

def process(frame):
	height, width, channels = frame.shape # laptop cam is 480x640
        # plot bisecting lines horizontally and vertically
        cv2.line(frame,(0,height/2),(width,height/2),(255,0,0),2)
        cv2.line(frame,(width/2,0),(width/2,height),(255,0,0),2)
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

        min = int(height * 0.2)
        uppers = upperbody_cascade.detectMultiScale(gray, 1.1, 1, minSize=(min,min))
        count = 0
        for (x,y,w,h) in uppers:
            count += 1
            cv2.rectangle(frame,(x,y),(x+w,y+h),(0,0,255),2)
            # plot centre of detection square
            a = x+(w/2)
            b = y+(h/2)
            cv2.rectangle(frame,(a-5,b-5),(a+5,b+5),(255,0,255),5)

            if count == 1:
                if a > width / 2: print("turn left")
                else: print("turn right")
                if b > height / 2: print("forward")
                else: print("backward")

        cv2.imshow("frame2", frame)
        #print("%s detected" % count)
        left = 0
        right = 0
        if cv2.waitKey(1) & 0xFF == ord('q'):
        break
        if (get_front_distance() > 510):
                arduino.write("<" + str(left) + "," + str(right) + ">")
        else:
                arduino.write("<0,0>")

	cv2.imshow("frame", frame)

while True:
	start = time.time()
	frame = vs.read()
	process(frame)
	end = time.time()
	fps = 1/(end-start)
	print(str(fps))

	if cv2.waitKey(1) & 0xFF == ord("q"):
		break

vs.stop()
cap.release()
cv2.destroyAllWindows()