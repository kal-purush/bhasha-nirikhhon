import cv2
import mediapipe as mp
import time
from auto_message_bot import sendmessage
import pyautogui


cap = cv2.VideoCapture(0)
max_num_hands = 3
mpHands = mp.solutions.hands
hands = mpHands.Hands()
mpDraw = mp.solutions.drawing_utils
hand_ring = -0.0001
prevTime = 0
currTime = 0
sent_msg = False
sent_msg_time = 0
while True:
    success, img = cap.read()
    imgRGB = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    results = hands.process(imgRGB)

    if results.multi_hand_landmarks:
        for x in results.multi_hand_landmarks:
            for id, lm in enumerate(x.landmark):
                #if id == 12:
                   # if hand_ring > float((str(lm).split())[5]):

                      #  pyautogui.click()
                    #else:
                       # hand_ring = float((str(lm).split())[5])

                if id == 8:
                    cords = (str(lm).split())
                    x1 = float(cords[1])*2000
                    y = float(cords[3])*1000
                    z = float(cords[5])
                    pyautogui.moveTo(x1, y)
                    if z < -0.26 and sent_msg == False:
                        pyautogui.click()
                        print('clicked')
                        sendmessage()
                        sent_msg = True
                        sent_msg_time = time.time()
                mpDraw.draw_landmarks(img, x, mpHands.HAND_CONNECTIONS)

    currTime = time.time()
    #print(currTime-prevTime)
    fps = 1/(currTime-prevTime)
    #print(fps)
    prevTime = currTime

    if currTime > sent_msg_time:

        sent_msg = False
    cv2.putText(img, str(int(fps)), (10,70), cv2.QT_FONT_BLACK, 2, (255, 20, 255), 4)


    cv2.imshow('Image', img)
    cv2.waitKey(1)