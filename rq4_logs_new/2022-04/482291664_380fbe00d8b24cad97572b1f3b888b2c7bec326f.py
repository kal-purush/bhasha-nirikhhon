from datetime import datetime
import cv2
import os

filename1 = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
count = 0
# cap = cv2.VideoCapture('rtsp://admin:123456789q@192.168.72.70:554/cam/realmonitor?channel=1&subtype=0')
face_cascade = cv2.CascadeClassifier('haarcascade_frontalface_alt2.xml')
# Check if the webcam is opened correctly
cap = cv2.VideoCapture(0)
if not cap.isOpened():
	raise IOError("Cannot open webcam")

if not os.path.exists('skrin'):
	os.mkdir('./skrin')

while cap.isOpened():
	ret, frame = cap.read()
	gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
	faces = face_cascade.detectMultiScale(gray, 1.1, 4)
	cv2.imshow("image", frame)
	fps = cap.get(cv2.CAP_PROP_FPS)
	cv2.waitKey(1)
	multiplier = fps * 60
	frame_id = int(round(cap.get(1)))
	# print(fps)
	for (x, y, w, h) in faces:
		cv2.rectangle(frame, (x, y), (x + w, y + h), (255, 0, 0), 2)

		if faces in faces:
			if frame_id % multiplier == 0:
				cv2.resize(frame, None, fx=0.5, fy=0.5, interpolation=cv2.INTER_AREA)
				cv2.imwrite(f"skrin/{filename1,  count}.jpg", frame)
				print(f"Saved {filename1, count}")
				count += 1
			else:
				print('no face')
				continue

	if cv2.waitKey(1) & 0xFF == ord('q'):
		break
cap.release()