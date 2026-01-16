import cv2
import time 
import mediapipe as mp
class HandDetector:
    def __init__(self,mode=False,maxhands=2,detconf=0.5,trkconf=0.5):
        self.mode=mode
        self.maxhands=maxhands
        self.detconf=detconf
        self.trkconf=trkconf
        self.mpHands= mp.solutions.hands # calling the solutions ,inside it the hands module
        self.hands=self.mpHands.Hands( static_image_mode=self.mode, 
        max_num_hands=self.maxhands, 
        min_detection_confidence=self.detconf, 
        min_tracking_confidence=self.trkconf) # Hands class Loads the pretrained model trained on huge datasets of hand images in 3D which detect hand in image or in the real time and predict 21 key landmarks points on hand
        self.mpDraw=mp.solutions.drawing_utils # draw the predicted landmarks and connection on the image
    def findhand(self,img,draw=True):
        imgRGB=cv2.cvtColor(img,cv2.COLOR_BGR2RGB)
        self.result=self.hands.process(imgRGB) # Process the image with hand detection and 21 key landmarks prediction on the hand 
        if draw:
            if self.result.multi_hand_landmarks:
                for handlmks in self.result.multi_hand_landmarks:
                    self.mpDraw.draw_landmarks(img,handlmks,self.mpHands.HAND_CONNECTIONS)
        return img
    def findposition(self,img,handsno=0,draw=True):
        lists=[]
        if self.result.multi_hand_landmarks:
            result=self.result.multi_hand_landmarks[handsno]
            for id,lm in enumerate(result.landmark):
                # print(f"Landmark{id}:\n{lm}")
                h,w,c=img.shape
                cx,cy=int(lm.x*w),int(lm.y*h)
                # print(f"\nLandmark{id}:\n{cx} and {cy}")
                lists.append([id,cx,cy])
                if draw:
                    if id==4 or id==8:
                        cv2.circle(img,(cx,cy),15,(255,255,0),cv2.FILLED)
        return lists
    

    
def main():
    cap=cv2.VideoCapture(0)
    current_time,previous_time=0,0
    detector=HandDetector() 
    while True:
        success,img=cap.read()
        img=detector.findhand(img)
        lists=detector.findposition(img)
        if len(lists)!=0:
            print(lists[4])
        current_time=time.time()
        fps=1/(current_time-previous_time)
        previous_time=current_time
        cv2.putText(img,str(int(fps)),(10,70),cv2.FONT_HERSHEY_COMPLEX,3,(255,255,255),3)
        cv2.imshow("Image",img)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
    cap.release() #closes the webcam.
    cv2.destroyAllWindows() #closes the OpenCV window.
    


if __name__=='__main__':
    main()