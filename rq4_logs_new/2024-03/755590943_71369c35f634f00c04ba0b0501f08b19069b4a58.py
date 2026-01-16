
import cv2
import numpy as np

from djitellopy import Tello

import time

#tello = Tello()

#tello.connect()
#print(f'Battery: {tello.get_battery()}%')

#tello.streamon()
time.sleep(3)

# Load YOLO
net = cv2.dnn.readNet("skyguide/yolo/yolov3.weights", "skyguide/yolo/darknet/cfg/yolov3.cfg")
layer_names = net.getLayerNames()
output_layers = [layer_names[i - 1] for i in net.getUnconnectedOutLayers()]

# Load video
#cap = cv2.VideoCapture(f'udp://192.168.10.1:11111')
cap = cv2.VideoCapture(1)
# cap = cv2.VideoCapture(f'skyguide/yolo/video.mp4')

# Use the first frame to get the width and height
ret, test_frame = cap.read()
height, width, channels = test_frame.shape

# Re-open the video file to start from the first frame
output = cv2.VideoWriter(filename='output.mp4', fourcc=cv2.VideoWriter_fourcc(*'mp4v'), fps=20.0, frameSize=(width, height))


#tello.takeoff()
pfound = False
while True:
    ret, frame = cap.read()

    if ret:
        # Detecting objects
        blob = cv2.dnn.blobFromImage(
            image=frame, 
            scalefactor=0.00392, 
            size=(64, 64),  # Changed to a common size for YOLO
            mean=(0, 0, 0),
            swapRB=True,
            crop=False
        )
        net.setInput(blob)
        outs = net.forward(output_layers)

        # Processing detections
        class_ids = []
        confidences = []
        boxes = []
        for out in outs:
            for detection in out:
                scores = detection[5:]
                class_id = np.argmax(scores)
                confidence = scores[class_id]
                if confidence > 0.5:
                    # Object detected
                    center_x = int(detection[0] * width)
                    center_y = int(detection[1] * height)
                    w = int(detection[2] * width)
                    h = int(detection[3] * height)

                    # Rectangle coordinates
                    x = int(center_x - w / 2)
                    y = int(center_y - h / 2)

                    boxes.append([x, y, w, h])
                    confidences.append(float(confidence))
                    class_ids.append(class_id)

        indexes = cv2.dnn.NMSBoxes(boxes, confidences, 0.5, 0.4)
        if len(indexes) > 0:
            for i in indexes.flatten():
                x, y, w, h = boxes[i]
                if str(class_ids[i]) == '0':  # Assuming '0' is the class ID for people
                    pfound = True
                    cv2.rectangle(frame, (x, y), (x + w, y + h), (255, 0, 0), 2)
                    #cv2.rectangle(image, (x, y), (x + w, y + h), (36,255,12), 1)
                    cv2.putText(frame, 'Fedex', (x, y-10), cv2.FONT_HERSHEY_SIMPLEX, 0.9, (36,255,12), 2)
        
        if pfound == False:
            print("rotate camera")
            
        else :
            #print("x:" + str(x) + "     x+w:" + str(x+w))
            #print("width:   " + str(width))

            midscreen = width/2
            midbox = x + w/2
            #if box is to the left of the midpoint
            if (midbox < midscreen):
                if (midbox + w/2 < midscreen):
                    print("rotate left")
                else:  
                    print("centered")
            else :
                if (midbox - w/2 > midscreen):
                    print("rotate right")
                else:  
                    print("centered")

        pfound = False

        output.write(frame)
        cv2.imshow('Frame', frame)

        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

#tello.streamoff()
cap.release()
output.release()
cv2.destroyAllWindows()