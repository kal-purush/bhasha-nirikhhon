import cv2
 

cam=cv2.VideoCapture(0)
 
car_cascade = cv2.CascadeClassifier('cars.xml')
 
while True:
    ret, frames = cam.read()
     
    gray = cv2.cvtColor(frames, cv2.COLOR_BGR2GRAY)
     
 
    cars = car_cascade.detectMultiScale(gray, 1.1, 1)
     
    for (x,y,w,h) in cars:
        cv2.rectangle(frames,(x,y),(x+w,y+h),(255,0,0),2)
 
    cv2.imshow('video2', frames)
     
    if cv2.waitKey(33) == 27:
        break
 
cv2.destroyAllWindows()