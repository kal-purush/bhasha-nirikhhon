import cv2
import numpy as np
import logging

cap = cv2.VideoCapture('/home/ydbondt/Downloads/IMG_0607-non-HEVC.mov')
fourcc = cv2.cv.CV_FOURCC(*'DIVX')

if (cap.isOpened() == False):
    print("Error")

ret, first_frame = cap.read()
out = cv2.VideoWriter('output.avi', fourcc, 20.0, (first_frame.shape[1], first_frame.shape[0]))

history_points = []
while (cap.isOpened() == True):
    ret, frame = cap.read()
    if ret == True:
        
        hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)
        lower_orange = np.array([0, 162, 186])
        upper_orange = np.array([46, 242, 255])

        rangeMask = cv2.inRange(hsv, lower_orange, upper_orange)

        contours, hierarchy = cv2.findContours(rangeMask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE);

        for c in contours:
            M = cv2.moments(c)
            perimeter = cv2.arcLength(c, True)
            print(perimeter)
            if M["m00"] != 0 and perimeter > 100:
                cv2.drawContours(frame, [c], -1, (0,255,0), 3)
                cX = int(M["m10"] / M["m00"])
                cY = int(M["m01"] / M["m00"])
                point = (cX, cY)
                history_points.append(point)
        prev_point = None
        for p in history_points:
            if (prev_point is None):
                prev_point = p

            cv2.line(frame, prev_point, p, (0,255,0), 2)
            prev_point = p

        out.write(frame)
        cv2.imshow('Frame', frame);

        if cv2.waitKey(1) & 0xFF == ord('q'):  # press q to quit
            break
    else:
        break

cap.release()
out.release()
cv2.destroyAllWindows()