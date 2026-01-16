import cv2
import numpy as np
import sys
from PIL import Image
class LocationFinder():
        def __init__(self,template,img):
            self.template = Image.open(template)
            self.img = cv2.imread(img)
            self.kernel = np.ones((2,2),np.uint8)
            
            
        def finder(self):
            w, h = self.template.size
            res = cv2.matchTemplate(self.img, cv2.cvtColor(np.array(self.template), cv2.COLOR_RGB2BGR),cv2.TM_CCOEFF_NORMED)
            min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(res)
            if max_val > 0.99:
                cv2.rectangle(self.img, max_loc, (max_loc[0] + w, max_loc[1] + h), (0,255,0), 2)
                self.img = cv2.resize(self.img,(800,600))
                cv2.imshow("result",self.img)
                cv2.waitKey(0)
                cv2.destroyAllWindows()
                return print(f'Location: {max_loc[0]}:{max_loc[0]+w},{max_loc[1]}:{max_loc[1]+h}')
            else:
                locImage = self.rotator(self.template)
                cv2.rectangle(self.img, locImage, (locImage[0] + w, locImage[1] + h), (0,255,0), 2)
                self.img = cv2.resize(self.img,(800,600))
                cv2.imshow("result",self.img)
                cv2.waitKey(0)
                cv2.destroyAllWindows()
                return print(f'Location: {locImage[0]}:{locImage[0]+w},{locImage[1]}:{locImage[1]+h}')
            
        
        def rotator(self,img):
            print("Rotator started")
            inVal = 0
            arr = []
            for i in range(0,361,5):
                a = img.copy().rotate(i)
                opencvImage = cv2.cvtColor(np.array(a), cv2.COLOR_RGB2BGR)
                opening = cv2.morphologyEx(opencvImage, cv2.MORPH_OPEN, self.kernel)
                res = cv2.matchTemplate(self.img,opening,cv2.TM_CCOEFF_NORMED)                
                min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(res)
                if max_val > 0.5 and max_val >= inVal:
                    print(f'max_val = {max_val} and i: {i}')
                    arr = [min_val, max_val, min_loc, max_loc]
                    inVal = max_val
            if arr[1] < 0.5:    
                raise Exception("Sorry, Given template not found in the image")
            return arr[3]
            
            