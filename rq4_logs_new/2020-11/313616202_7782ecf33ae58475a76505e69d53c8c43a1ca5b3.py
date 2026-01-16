#import module
import cv2
import numpy as np
import utlis

curvelist=[]#curve value
avgVal=10#average value for list for curvelist

def getLaneCurve(img,display=2):#input image, display=0:nothing, display=1:just the result, display=2:all the things
    imgCopy=img.copy()
    imgResult=img.copy()
    imgThres = utlis.thresholding(img)#output image

    hT,wT,c=img.shape
    points = utlis.valTrackbars()
    imgWarp= utlis.warpImg(imgThres,points,wT,hT)#output warped image with black and white
    imgWarpPoints=utlis.drawPoints(imgCopy,points)

    #path mid point
    midPoint,imgHist=utlis.getHistogram(imgWarp,display=True,minPer=0.5,region=4)
    #region=1/4,means the bottom 1/4 are being counted
    #minPer=0.5,means minimum Percent,make too small numbers do not consider as a part of our path

    #basepoint
    curveAveragePoint, imgHist = utlis.getHistogram(imgWarp, display=True, minPer=0.9)#region=1
    curveRaw=curveAveragePoint-midPoint#our curve value

    #make the path value list
    curvelist.append(curveRaw)
    if len(curvelist)>avgVal:
        curvelist.pop(0)
    curve=int(sum(curvelist)/len(curvelist))

    #Display
    if display != 0:
        imgInvWarp = utlis.warpImg(imgWarp, points, wT, hT, inv=True)
        imgInvWarp = cv2.cvtColor(imgInvWarp, cv2.COLOR_GRAY2BGR)
        imgInvWarp[0:hT // 3, 0:wT] = 0, 0, 0
        imgLaneColor = np.zeros_like(img)
        imgLaneColor[:] = 0, 255, 0
        imgLaneColor = cv2.bitwise_and(imgInvWarp, imgLaneColor)
        imgResult = cv2.addWeighted(imgResult, 1, imgLaneColor, 1, 0)
        midY = 450
        cv2.putText(imgResult, str(curve), (wT // 2 - 80, 85), cv2.FONT_HERSHEY_COMPLEX, 2, (255, 0, 255), 3)
        cv2.line(imgResult, (wT // 2, midY), (wT // 2 + (curve * 3), midY), (255, 0, 255), 5)
        cv2.line(imgResult, ((wT // 2 + (curve * 3)), midY - 25), (wT // 2 + (curve * 3), midY + 25), (0, 255, 0), 5)
        for x in range(-30, 30):
            w = wT // 20
            cv2.line(imgResult, (w * x + int(curve // 50), midY - 10),
                     (w * x + int(curve // 50), midY + 10), (0, 0, 255), 2)
        #fps = cv2.getTickFrequency() / (cv2.getTickCount() - timer);
        #cv2.putText(imgResult, 'FPS ' + str(int(fps)), (20, 40), cv2.FONT_HERSHEY_SIMPLEX, 1, (230, 50, 50), 3);
    #stuck all the image together
    if display == 2:
        imgStacked = utlis.stackImages(0.7, ([img, imgWarpPoints, imgWarp],
                                             [imgHist, imgLaneColor, imgResult]))
        cv2.imshow('ImageStack', imgStacked)
    elif display == 1:
        cv2.imshow('Resutlt', imgResult)


    #test individually
    #cv2.imshow('Thres',imgThres)
    #cv2.imshow('Warp', imgWarp)
    #cv2.imshow('Warp Points', imgWarpPoints)
    #cv2.imshow('Histogram', imgHist)

    #normalization
    curve=curve//100
    if curve>-1:
        curve=1
    if curve<-1:
        curve=-1
    print(curve)
    return curve

if __name__ == '__main__':
    cap = cv2.VideoCapture('last.h264')
    #put the points value in
    intialTracbarVals = [60, 200, 0, 255]#################### NEEEED TO ADJUST use trackbars!!!!!!!
    utlis.initializeTrackbars(intialTracbarVals)

frameCounter=0 #for the loop

while True:
    #looping for Warped img
    frameCounter += 1
    if cap.get(cv2.CAP_PROP_FRAME_COUNT) == frameCounter:
        cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
        frameCounter = 0

    success, img = cap.read() # GET THE IMAGE
    img = cv2.resize(img,(480,240)) # RESIZE
    getLaneCurve(img)#
    #curve=getLaneCurve(img,display=0/1/2)only result/trackbars/everything
    cv2.imshow('Vid',img)# SHOW THE VIDEO
    #wait user
    cv2.waitKey(1)