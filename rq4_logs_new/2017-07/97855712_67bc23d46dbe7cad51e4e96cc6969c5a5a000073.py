# -*- coding: utf-8 -*-
"""
Created on Thu Mar 02 13:54:02 2017

@author: Matthew Horton
"""
from nplab.instrument.stage.prior import ProScan
from nplab.instrument.camera.lucam import Lucam, API
from nplab.instrument.camera import CameraParameter

from nplab.instrument.camera.Andor.__init__ import Andor
import matplotlib.pyplot as plt
from PIL import Image

from time import sleep
import numpy

def __init__():
    cam = Lucam()
    cam.__init__()
    
    camAndor = Andor()
    camAndor.__init__()
    
    for pname in cam.PROPERTY.keys():
        setattr(cam, pname, CameraParameter(pname)) 
    stage = ProScan()   
    stage.__init__()
    return camAndor, cam, stage

def SingleShot(cam,filename='test.bmp'):
   # Set camera settings.
    frameformat = cam.GetFormat()[0]
    snapshot = cam.Snapshot(exposure=cam.GetProperty('exposure')[0],
                            gain=cam.GetProperty('gain')[0]/2,
                            timeout=cam.GetProperty('exposure')[0] + 500.0,
                            format=frameformat)
    # Take and save data.                       
    data = cam.TakeSnapshot(snapshot)
    colorData = cam.ConvertFrameToRgb24(frameformat,
                                        data.ctypes.data_as(API.pBYTE))
    numpyColorData = numpy.ctypeslib.as_array(colorData, shape = None)
    numpyColorDataT = numpy.rollaxis(numpyColorData,2)
    cam.SaveImage(numpyColorDataT,filename)
    
def LinScan(stage, cam, xrng, xstp, sleeptime, picNameArray):
    if xstp == 0:
        SingleShot(cam,picNameArray[0])
    else:
        for x in range(xstp):
            # Take a picture before making first move.
            if (x < 1) & (xrng != 0):
                sleep(sleeptime)
                SingleShot(cam,picNameArray[x])
            # Then, take pics after moving.
            stage.move_rel([xrng/xstp, 0, 0])
            sleep(sleeptime)
            SingleShot(cam,picNameArray[x+1])
        # Return to start position.
        stage.move_rel([-xrng, 0, 0])

def AreaScan(stage, cam, xrng, xstp, yrng, ystp, sleeptime, picNameArray):
    if ystp == 0:
        LinScan(stage, cam, xrng, xstp, sleeptime, picNameArray[0])
    else:    
        for y in range(ystp):
            # Take a scan before making first move.
            if (y < 1) & (yrng != 0):
                LinScan(stage, cam, xrng, xstp, sleeptime, picNameArray[y])
            # Then, take scans after moving.
            stage.move_rel([0, yrng/ystp, 0])
            LinScan(stage, cam, xrng, xstp, sleeptime, picNameArray[y+1])
        # Return to start position.
        stage.move_rel([0, -yrng, 0])

def VolumeScan(stage, cam, xrng, xstp, yrng, ystp,
               zrng, zstp, sleeptime, picNameArray):
    if zstp == 0:
        AreaScan(stage, cam, xrng, xstp,
                 yrng, ystp, sleeptime, picNameArray[0])
    else:  
        for z in range(zstp):
            # Take a scan before making first move.
            if (z < 1) & (zrng != 0):
                AreaScan(stage, cam, xrng, xstp,
                         yrng, ystp, sleeptime, picNameArray[z])
            # Then, take scans after moving.
            stage.move_rel([0,0,zrng/zstp])
            AreaScan(stage, cam, xrng, xstp,
                     yrng, ystp, sleeptime, picNameArray[z+1])
        # Return to start position.
        stage.move_rel([0,0,-zrng])

def GenerateLinNameArray(scanName, xrng, xstp, strFileType, boolFileType):
    # Initialize array of names.
    picNameArray = [scanName for x in numpy.linspace(0, xrng, xstp)]
    # Add labels & numbers to names.
    for x in range(xstp):
        picNameArray[x] = (picNameArray[x] + "X"
        + str(x).zfill(int(numpy.ceil(numpy.log10(len(picNameArray))))))
    # If necessary, add the filetype to the names.
    if boolFileType:
        picNameArray = [s + strFileType for s in picNameArray]
    return picNameArray

def GenerateAreaNameArray(scanName, xrng, xstp,
                          yrng, ystp, strFileType, boolFileType):
    # Initialize array of names.
    picNameArray = [GenerateLinNameArray(scanName, xrng, xstp,
                                         strFileType, False)
                    for y in numpy.linspace(0, yrng, ystp)]
    # Add labels & numbers to names.
    for y in range(ystp):
        picNameArray[y] = [picNameArray[y][s] + "Y" 
        + str(y).zfill(int(numpy.ceil(numpy.log10(len(picNameArray[y])))))
                            for s in range(xstp)]
    # If necessary, add the filetype to the names.
    if boolFileType:
        picNameArray = [[s + strFileType for s in t] for t in picNameArray]   
    return picNameArray

def GenerateVolumeNameArray(scanName, xrng, xstp, yrng, ystp,
                            zrng, zstp, strFileType, boolFileType):
    # Initialize array of names.
    picNameArray = [GenerateAreaNameArray(scanName, xrng, xstp,
                                          yrng, ystp, strFileType, False)
                    for z in numpy.linspace(0, zrng, zstp)]
    # Add labels & numbers to names.
    for z in range(zstp):
        picNameArray[z] = [[picNameArray[z][t][s] + "Z"
        + str(z).zfill(int(numpy.ceil(numpy.log10(len(picNameArray[z])))))
                            for t in range(xstp)]
                            for s in range(ystp)]    
    # If necessary, add the filetype to the names.
    if boolFileType:
        picNameArray = [[[s + strFileType for s in t]
                        for t in u]
                        for u in picNameArray]
    return picNameArray

if __name__ == '__main__':
    camAndor, cam, stage = __init__()
    xrng, yrng, zrng = 3,2,1
    xstp, ystp, zstp = 3,2,1
    sleeptime = 1
    scanName = "Test"
    strFileType = ".bmp"
    
    
    # Take and save Andor data.                       
    camAndor.raw_snapshot()
    data = camAndor.imageArray
    img = [data[i:i+1600] for i in xrange(0, len(data), 1600)]
    print(numpy.size(img))
    imgplot = plt.imshow(img)
    plt.show()
    
    picNameArray = GenerateAreaNameArray(scanName, xrng, xstp+1,
                                         yrng, ystp+1, strFileType, True)
    print(picNameArray)
    AreaScan(stage, cam, xrng, xstp, yrng, ystp, sleeptime, picNameArray)
    print('Done.')
    