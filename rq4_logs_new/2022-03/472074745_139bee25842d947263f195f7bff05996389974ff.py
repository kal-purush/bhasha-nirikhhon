import cv2
import mediapipe as mp
import time

cap = cv2.VideoCapture(0) # create a video object

mpHands = mp.solutions.hands
hands = mpHands.Hands() #check parameters for modifications
                        #static_image_mode=False,
                        #max_num_hands=2,
                        #model_complexity=1,
                        #min_detection_confidence=0.5,
                        #min_tracking_confidence=0.5):
mpDraw = mp.solutions.drawing_utils
                            
pTime = 0
cTime = 0

while True:
    success, img = cap.read() # gives the frame BGR
    imgRGB = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    results = hands.process(imgRGB)
    #print(results.multi_hand_landmarks)    
    if results.multi_hand_landmarks:
        for handlms in results.multi_hand_landmarks:
            for id, lm in enumerate(handlms.landmark):
                #print(id,lm)
                height, width, channels = img.shape
                cx, cy = int(lm.x*width), int(lm.y*height)
                #print(id, cx, cy)
                if id == 0:
                    cv2.circle(img,(cx,cy),15,(255,0,255),cv2.FILLED)
            mpDraw.draw_landmarks(img, handlms, mpHands.HAND_CONNECTIONS)
         
            
    cTime = time.time()
    fps = 1/(cTime-pTime)
    pTime = cTime
    
    cv2.putText(img, 
                str(int(fps)),
                (15,50), 
                cv2.FONT_HERSHEY_PLAIN,
                2,(255,0,255),2)
    cv2.imshow("image", img) # standard procedure to run a webcam
    cv2.waitKey(1) # standard procedure to run a webcam 