import array
import math
from ola.ClientWrapper import ClientWrapper
import cv2
import numpy

def DmxSent(state):
  wrapper.Stop()

scale = 1
universe = 0
loop_count = 0
data = array.array('B')

cv2.namedWindow("Video",1)
#cap = cv2.VideoCapture('FD0A0886__.mov')

cap = cv2.VideoCapture(2)

width = int(cap.get(3))
height = int(cap.get(4))





print height, width

ret, frame = cap.read()
frame_darker = (frame * scale).astype(numpy.uint8)

while ret: 
    wrapper = ClientWrapper()
    client = wrapper.Client()
    gray = cv2.cvtColor(frame_darker, cv2.COLOR_BGR2GRAY) #convert to grayscale
    res = cv2.resize(gray,(width/4, height/4), interpolation = cv2.INTER_CUBIC)

    for y in xrange(0,height/4-1):
      universe = y
      for x in xrange(0,width/4-1):
        data.append(res[y][x])
      client.SendDmx(universe, data, DmxSent)  
      data = array.array('B')


    #print gray
    cv2.imshow("Video", res)
    cv2.waitKey(1) # time to wait between frames, in mSec
    ret, frame = cap.read()
    frame_darker = (frame * scale).astype(numpy.uint8)


cap.release()
cv2.destroyAllWindows()
