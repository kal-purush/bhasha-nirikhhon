# test_opencv.py
#
# 

import sys

import cv2
import matplotlib.pyplot as plt
import numpy as np

from scipy import signal

####################################################################
# dumpJson(sFile, sOut, dX, dY, aOffsets)
#
def dumpJson(sFile,sOut,dX,dY,aOffsets):
  print ("{\n"
         " file:\"%s\",\n"
         " out:\"%s\",\n"
         " width:%d,\n"
         " height:%d,\n"
          % (sFile,sOut,dX, dY))
  sys.stdout.write(" offsets:[")
  if len(aOffsets):
    for i in range(len(aOffsets)-1):
      sys.stdout.write ("%d," % aOffsets[i])
    sys.stdout.write ("%d" % aOffsets[-1])
  print "]\n}"
  return

####################################################################
#
def gammaCorrect(img, fCor):
  img = img/255.0
  img = cv2.pow(img,fCor)
  return np.uint8(img*255)

####################################################################
# main()
#   args:   <inputFile> [-n]
#
def main():
  if len(sys.argv) < 2:
    print "Failure"
    exit()
  else:
    sImg = sys.argv[1]
  bLoud = False
  if len(sys.argv) > 2:
    bLoud = True if sys.argv[2] == '-l' else False

  if bLoud:
    print "Image is:",sImg
  
  imgIn = cv2.imread(sImg)
  if imgIn == None:
    print "Could not open image"
    exit()

  img = cv2.cvtColor(imgIn,cv2.COLOR_BGR2GRAY)

  dY, dX = img.shape[:2]
  nP = dX * dY

  if bLoud:
    cv2.imshow(sImg,img)
    cv2.waitKey(0)
  
# establish rough histogram to determine how to scale in brightness/contrast
  hist, bins = np.histogram(img.ravel(),256)

# sum bins from top to mid in order to determine rough brightness
  nX = 0
  for i in range(255,127,-1):
    nX += hist[i]
  if bLoud:
    print "Ratio of bright to total:", float(nX)/nP
  
  img = gammaCorrect(img, 2.0)
  if bLoud:
    cv2.imshow("gamma",img)
    cv2.waitKey(0)

  if dX > dY:
    fS = 800.0 / dX
  else:
    fS = 800.0 / dY

  imgCopy = img.copy()

  dXsample = 10
  dX2 = (dX/2) - dXsample

  accum = np.zeros(dY)
  line = np.zeros(dY)

  if bLoud:
    print "midStart, x:",dX2, " end:",dX2+dXsample
  for iX in range(dX2-dXsample,dX2+dXsample):
    for iY in range(dY):
      x = img[iY,iX]
      line[iY] = x
      accum[iY] += x
#  plt.plot(line)
#  plt.show()
    cv2.line(imgCopy,(iX,0),(iX,dY),(255,255,255),1)

  if bLoud:
    cv2.imshow("w line",imgCopy)

    plt.plot(accum)
    plt.title("Raw accum")
    plt.show()

  accum /= (2 * dXsample)

  if bLoud:
    plt.title("Scaled back")
    plt.plot(accum)
    plt.draw()
#  plt.show()
  
# finds negative peaks in the data

  accum = accum * -1
  peak = signal.find_peaks_cwt(accum,np.arange(1,25))
  for i in peak:
    if bLoud:
      print "at offset:",i," value:",accum[i]
    ii = accum[i] * -1
    if ii < 64:
      cv2.line(imgCopy,(0,i),(dX,i),(255,0,0),1)
    else:
      if bLoud:
        print "Point no good"

  if bLoud:
    cv2.imshow('test',imgCopy)
    plt.show()
    cv2.waitKey(0)

  sOut = sImg + ".out.jpg"

  cv2.imwrite(sOut,imgCopy)
  if bLoud:
    print peak

  dumpJson(sImg, sOut,dX, dY,peak)

#   print "at x,y",iX," ",iY," ",img[iY,iX]
  return

if __name__ == "__main__":
  main()

