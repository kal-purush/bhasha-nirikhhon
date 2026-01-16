import cv2
import numpy as np
import time

from keras.models import load_model
start = time.time()
DataDir = r"C:\Users\Thomacdebabo\Downloads\Dataset(1)\Dataset\Controll\2.jpg"
ModelName = "waldoBig5.hdf5"
ImgSize = 64
max = 100
thresh = 0.8

psf = 0.75

model = load_model(ModelName)

img = cv2.imread(DataDir, 1)
img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
img = cv2.resize(img, (0, 0), fx=psf, fy=psf, interpolation=cv2.INTER_CUBIC)
a = img.shape

stepsize = 32

xit = int((a[0]-ImgSize)/stepsize)
yit = int((a[1]-ImgSize)/stepsize)
pred = np.ones((xit,yit), dtype='float32')

for x in range(xit):
    for y in range(yit):
        pic1 = img[x*stepsize:x*stepsize+ImgSize,y*stepsize:y*stepsize+ImgSize]
        pic = np.array([pic1])
        pred[x,y]= model.predict(pic/255.)
        print model.predict(pic/255.)


 
z = 0
while pred[np.unravel_index(np.argmax(pred, axis=None), pred.shape)]>thresh:
    index = np.unravel_index(np.argmax(pred, axis=None), pred.shape)
    indexarr = np.asarray(index)
    
    print pred[index]
    pred[index]= 0.
    
    pt1 = indexarr*stepsize
    pt2 = indexarr*stepsize+[ImgSize,ImgSize]
    pt1 = (pt1[1],pt1[0])
    pt2 = (pt2[1],pt2[0])
    cv2.rectangle(img,pt1,pt2, (255-z,0,z),5)
    z = z+1
print z
end = time.time()
print ("time elapsed: " + str(end-start)+" seconds")

img = cv2.resize(img, (0, 0), fx=0.5, fy=0.5, interpolation=cv2.INTER_CUBIC)
img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)
cv2.imshow("Wo isch de Walti?", img)
cv2.waitKey(0)
cv2.destroyAllWindows()