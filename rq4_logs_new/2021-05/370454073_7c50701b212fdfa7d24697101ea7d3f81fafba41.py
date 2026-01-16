import cv2
import keyboard
import numpy as np
import time

cap = cv2.VideoCapture(0)

face_cascade = cv2.CascadeClassifier('haarcascade_frontalface_default.xml')

prev = (0,0)
curr = (0,0)

n = 1

while(cap.isOpened()):

	ret,frame = cap.read()
	frame = cv2.flip(frame,1)

	gray = cv2.cvtColor(frame,cv2.COLOR_BGR2GRAY)

	faces = face_cascade.detectMultiScale(gray,1.1,4)

	for x,y,w,h in faces:

		cv2.circle(frame,(x + int(w/2),y + int(h/2)),5,(0,255,0),2)

	cv2.imshow('frame',frame)


	if n==1:
		prev = (x+int(w/2),y+int(h/2))
		t1 = time.time()
		n = 2
	else:
		curr = (x+int(w/2),y+int(h/2))
		t2 = time.time()

		x1,y1 = prev
		x2,y2 = curr

		#print(x1,y1)
		#print(x2,y2)

		if(abs(x1-x2)>60 or abs(y1-y2)>60) and abs(t1-t2)>1:

			if(abs(x1-x2)-abs(y1-y2)>0):
				if(x1-x2>0):
					keyboard.press_and_release("left")
					print("left")

				else:
					keyboard.press_and_release("right")
					print("right")

			else:
				if(y1-y2>0):
					keyboard.press_and_release("up")
					print("up")

				else:
					keyboard.press_and_release("down")
					print("down")

			prev = curr
			t1 = t2

	if cv2.waitKey(1)==27:
		break

cap.release()
cv2.destroyAllWindows()
