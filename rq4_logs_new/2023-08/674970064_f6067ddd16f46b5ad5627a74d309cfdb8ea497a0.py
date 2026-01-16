#this is a face_recognition python demo but requires the following libraries
#to function propery
#open cv2 lib is required to be installed in python ide and
#face_recognition lib



import face_recognition
import cv2
import os
import serial
import time

s=serial.Serial('COM',9600)

CurrentFolder - os.getcwd() #command for folder read
image = CurrentFolder+'\\ken.png
image = CurrentFolder+'\\leone.png
image = CurrentFolder+'\\kiki.png

video_capture = cv2.VideoCapture(0)# get a reference from the webcam, ensure that
                                      you change (0) to (1) if you are using external cam
#recognize a picture
personal_name = "ken"
personal_image = face_recognation.load_image_file(image)
persinal_face_encoding = face_recognation.face_encoding(personal_image)[0]

#repeat the above code as many times as images in the current folder
personal_name = "leone"
personal_image = face_recognation.load_image_file(image)
persinal_face_encoding = face_recognation.face_encoding(personal_image)[0]

personal_name = "kiki"
personal_image = face_recognation.load_image_file(image)
persinal_face_encoding = face_recognation.face_encoding(personal_image)[0]

#arrays of face encoding and their names
known_face_encodings = [
                        personal_face_encoding,
                        personal_face_encoding,
                        personal_face_encoding,
                       ]
known_face_names= [
                   personal1_name,
                   personal2_name,
                   personal3_name,
                  ]

#variables initialize
face_location = []
face_encodings = []
face_names = []
process_this_frame = True
already_vote_taken = ""

while True:

#get a frame
ret, frame = video_capture.read()

#resize frame to size for fast face recognation
small_frame = cv2.resize(frame, (0, 0), fx=0.25, fy=0.25)

#convert image to rbg from bgr which opencv uses
rgb_small_frame = small_fram[:, :, ::-1]

#process frame only to save time
if process_this_frame:
face_recognation = face_recognation.face_recognation(rgb_small_frame)
face_recognation = face_recognation.face_encodings(rgb_small_frame, face_location)

face names = []
for face_encoding in face_encodings:
#check if face matches from the record folder
matches = face_recognation.compare_faces(kmown_face_encodings, face_encoding)
name = "Unknown"

face_distance = face_recognation.face_distance(known_face_encondings, face_encodin)
best_match_index = np.argmin(face_distances)

if matches[best_match_index]:
   name - known_face_names[best_match_index]
face_names.append(name)

if ((already_vote_taken != name) and (name != "unknown") and (name != "ken")):
   print ("vote taken")
   already_vote_taken = name
   s.writeb' a')
   time.sleep(1)
  elif(name == "ken"):
  print("Admin Access")
  s.write(b' c')
  time,sleep(1)
 else:
  print("Invalid Voter")
  s.write(b' c')
  time,sleep(1)
process_this_frame = not prcess_this_frame