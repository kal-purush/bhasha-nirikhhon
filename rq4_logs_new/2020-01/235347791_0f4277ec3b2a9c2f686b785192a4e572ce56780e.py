import cv2
import numpy as np
import tensorflow
MODEL=tensorflow.keras.models.load_model('emergency_cnn.model')
img=cv2.imread('1.jpeg',0)
cv2.imshow('frame',img)
img1=cv2.resize(img,(128,128))
print(img1.shape)
data1=np.asarray(img1,dtype="int32")
data2=data1/255.0
data3=np.array(data2).reshape(-1,128,128,1)
j=MODEL.predict([data3])
if j[0] >= 0.79:
    print('prediction : ambulance {0:.2f}%'.format(j[0][0]*100))
if j[0] < 0.79:
    print('prediction : fire engine {0:.2f}%'.format((1-j[0][0])*100))
cv2.waitKey(0)
cv2.destroyAllWindows()