import cv2
from djitellopy import Tello

import time

tello = Tello()

tello.connect()

keepRecording = True
tello.streamon()
time.sleep(3)

# Video capture
cap = cv2.VideoCapture('udp://192.168.10.1:11111')

# Load YOLO
net = cv2.dnn.readNet("skyguide/yolo/yolov3.weights", "skyguide/yolo/darknet/cfg/yolov3.cfg")
layer_names = net.getLayerNames()
output_layers = [layer_names[i - 1] for i in net.getUnconnectedOutLayers()]

# Re-open the video file to start from the first frame
output = cv2.VideoWriter(filename='output.mp4', fourcc=cv2.VideoWriter_fourcc(*'mp4v'), fps=20.0, frameSize=(480, 480))

if not cap.isOpened():
    print("Failed to read video stream")
else:
    while True:
        ret, frame = cap.read()
        if ret:
            # Display the resulting frame
            cv2.imshow('Tello Video Stream', frame)

            # Press Q on keyboard to exit
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break
        else:
            print("Failed to capture frame")
            break

tello.streamoff()