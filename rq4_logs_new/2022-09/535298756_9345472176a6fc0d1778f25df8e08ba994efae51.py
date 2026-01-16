import cv2
import numpy as np
import xlsxwriter 
from collections import Counter
from skimage.feature import greycomatrix, greycoprops
from skimage.measure import label, regionprops, regionprops_table
from sklearn.cluster import KMeans

workbook        =xlsxwriter.Workbook('fitur3.xlsx')
worksheet       =workbook.add_worksheet()

jenis           =['kering','tdkkering']
jum_per_data    =1001

hsv_properties  =['hue','saturation','value']

worksheet.write (0,0,'File')
kolom           =1

#writing excel header
for i in hsv_properties:
    worksheet.write(0,kolom,i)
    kolom+=1
worksheet.write(0,kolom,'Class')
kolom+=1
baris           =1

#looping for each dataset
for i in jenis:
    for j in range(1, jum_per_data):
        kolom       =0
        file_name   ="e:/PROPOSAL/sampel/sampelcitra/" + i + " ("+ str(j) +").jpg"
        print(file_name)
        worksheet.write(baris,kolom,file_name)
        kolom+=1
        
        #preprocessing
        src             =cv2.imread(file_name, 1)
        tmp             =cv2.cvtColor(src, cv2.COLOR_BGR2GRAY)
        _,mask          =cv2.threshold(tmp,75,255,cv2.THRESH_BINARY_INV)
        mask            =cv2.dilate(mask.copy(),None,iterations=10)
        b, g, r         =cv2.split(src)
        rgba            =[b,g,r, mask]
        dst             =cv2.merge(rgba, 4)

        contours, hierarcy =cv2.findContours(mask,cv2.RETR_TREE,cv2.CHAIN_APPROX_SIMPLE)
        selected        =max(contours,key=cv2.contourArea)
        x,y,w,h         =cv2.boundingRect(selected)
        cropped         =dst[y:y+h,x:x+w]
        mask            =mask[y:y+h,x:x+w]
        gray            =cv2.cvtColor(cropped, cv2.COLOR_BGR2GRAY)

        #hsv
        hsv_image       =cv2.cvtColor(cropped, cv2.COLOR_BGR2HSV)
        image           =hsv_image.reshape((hsv_image.shape[0] * hsv_image.shape[1], 3))
        clt             =KMeans(n_clusters = 3)
        labels          =clt.fit_predict(image)
        label_counts    =Counter(labels)
        dom_color      =clt.cluster_centers_[label_counts.most_common(1)[0][0]]
        
        worksheet.write(baris,kolom,dom_color[0])
        kolom+=1
        worksheet.write(baris,kolom,dom_color[1])
        kolom+=1
        worksheet.write(baris,kolom,dom_color[2])
        kolom+=1
        #dom_color_hsv  =[[1],[2],[3]]
        #dom_color_hsv  =np.full(cropped.shape, dom_color, dtype='uint8')
        #dom_color_bgr  =cv2.cvtColor(dom_color_hsv, cv2.COLOR_HSV2BGR)
        #output_image   =np.hstack((cropped, dom_color_bgr))
 
        #cv2.imshow('Image Dominant Color ', output_image)
        cv2.waitKey(0)
        
        
        worksheet.write(baris,kolom,i)
        kolom+=1
        baris+=1
workbook.close()