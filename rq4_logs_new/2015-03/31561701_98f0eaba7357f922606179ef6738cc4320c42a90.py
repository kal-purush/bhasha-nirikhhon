__author__ = 'Ryan'
from math import pi
import random
import os
from helpers import *


locations = [[0.5, 0.9, 0.5],[0.4081, -0.1, 0.221]]

yaws = [pi/2]
pitchs = [pi/15]
rolls = [0, pi/2]



baseLocation = 'C:\\Users\\Ryan\\Dropbox\\Research\\PycharmProjects\\Scrap\\TiO2Surface\\CONTCAR'
moleculeLocation = 'C:\\Users\\Ryan\\Dropbox\\Research\\PycharmProjects\\Scrap\\Molecules\\DIMETHYLAMINE'
foldersLocation = 'C:\\Users\\Ryan\\Dropbox\\Research\\PycharmProjects\\Scrap\\Folders'

# baseLocation = 'D:\\My Stuff\\Dropbox\Dropbox\\Research\\PycharmProjects\\Scrap\\TiO2Surface\\CONTCAR'
# moleculeLocation = 'D:\\My Stuff\\Dropbox\Dropbox\\Research\\PycharmProjects\\Scrap\\Molecules\\BENZENE'
# foldersLocation = 'D:\\My Stuff\\Dropbox\Dropbox\\Research\\PycharmProjects\\Scrap\\Folders'
fileName = 'POSCAR'




i = 0
for location in locations:
    location = NP.matrix(location)
    for yaw in yaws:
        for pitch in pitchs:
            for roll in rolls:

                base = loadPosFile(baseLocation)
                molecule = loadPosFile(moleculeLocation)
                yaw = NP.longdouble(yaw)
                pitch = NP.longdouble(pitch)
                roll = NP.longdouble(roll)
                saveLocation = os.path.join(foldersLocation, str(i), fileName)
                try:
                    os.makedirs(os.path.join(foldersLocation, str(i)))
                except:
                    print('Folder Already Exists, contents overwritten at' + saveLocation)
                savePosfile(addAndRotate(base,molecule,location, roll, pitch, yaw), saveLocation)
                i += 1