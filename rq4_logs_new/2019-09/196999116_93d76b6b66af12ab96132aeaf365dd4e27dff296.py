
import face_recognition
import cv2
import numpy as np
import attendance_logger as al
from tkinter import messagebox
import tkinter as tk
from functools import partial
from datetime import date

def Detect_face():
    flag=0
    video_capture = cv2.VideoCapture(0)

    vijay_image = face_recognition.load_image_file("knownfaces/VIJAY.jpg")
    vijay_face_encoding = face_recognition.face_encodings(vijay_image)[0]
    vincent_image = face_recognition.load_image_file("knownfaces/VINCENT.jpg")
    vincent_face_encoding = face_recognition.face_encodings(vincent_image)[0]
    rachel_image = face_recognition.load_image_file("knownfaces/RACHEL.jpg")
    rachel_face_encoding = face_recognition.face_encodings(rachel_image)[0]
    thania_image = face_recognition.load_image_file("knownfaces/THANIA.jpg")
    thania_face_encoding = face_recognition.face_encodings(thania_image)[0]
    navis_image = face_recognition.load_image_file("knownfaces/NAVIS.jpg")
    navis_face_encoding = face_recognition.face_encodings(navis_image)[0]
    rinin_image = face_recognition.load_image_file("knownfaces/RININ.jpg")
    rinin_face_encoding = face_recognition.face_encodings(rinin_image)[0]
    known_face_encodings = [
        vijay_face_encoding , vincent_face_encoding , rachel_face_encoding , thania_face_encoding , navis_face_encoding , rinin_face_encoding
        ]
    known_face_names = [
        "PV" , "vincent" , "Rachel" , "taniha" , "navis" , "rinin"
    ]

    face_locations = []
    face_encodings = []
    face_names = []
    process_this_frame = True

    while True:
        ret, frame = video_capture.read()
        small_frame = cv2.resize(frame, (0, 0), fx=0.25, fy=0.25)
        # Convert the image from BGR color (which OpenCV uses) to RGB color (which face_recognition uses)
        rgb_small_frame = small_frame[:, :, ::-1]
        if process_this_frame:
            face_locations = face_recognition.face_locations(rgb_small_frame)
            face_encodings = face_recognition.face_encodings(rgb_small_frame, face_locations)

            face_names = []
            for face_encoding in face_encodings:
                matches = face_recognition.compare_faces(known_face_encodings, face_encoding)
                name = "Unknown"
                face_distances = face_recognition.face_distance(known_face_encodings, face_encoding)
                best_match_index = np.argmin(face_distances)
                if matches[best_match_index]:
                    name = known_face_names[best_match_index]
                    flag=1

                face_names.append(name)

        process_this_frame = not process_this_frame
        for (top, right, bottom, left), name in zip(face_locations, face_names):
            top *= 4
            right *= 4
            bottom *= 4
            left *= 4
            cv2.rectangle(frame, (left, top), (right, bottom), (0,255,0), 2)
            cv2.rectangle(frame, (left, bottom - 35), (right, bottom), (255, 0,0), cv2.FILLED)
            font = cv2.FONT_HERSHEY_DUPLEX
            cv2.putText(frame, name, (left + 6, bottom - 6), font, 1.0, (255, 255, 255), 1)
            
            if(flag):
                print("Welcome To class",name)
                today = date.today()
                d1 = today.strftime("%d/%m/%Y")
                message= "The attandance for "+name+"\n on "+d1+" has been marked"
                messagebox.showinfo("Attendance",message)
                al.Mark_attendance(name)
            break
        cv2.imshow('Video', frame)
        if (cv2.waitKey(1) and 0xFF == ord('q')) or flag:
            break

    video_capture.release()
    cv2.destroyAllWindows()
