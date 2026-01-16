from pyqtgraph.Qt import QtCore, QtGui
import pyqtgraph as pg
import pyqtgraph.opengl as gl
import numpy as np
import itertools as iTt

resolutionForPho = 5
bxSdLn = 4*np.pi
a = bxSdLn/(2*np.sqrt(2))
scl = 1
org = np.array([0,0,0])

xpre = np.array([0,1,0,0,1,1,0,1,0.5,0.5,1,0.5,0,0.5,0.25,0.75,0.25,0.75])
ypre = np.array([0,0,1,0,1,0,1,1,0.5,0.5,0.5,1,0.5,0,0.25,0.75,0.75,0.25])
zpre = np.array([0,0,0,1,0,1,1,1,0,1,0.5,0.5,0.5,0.5,0.25,0.25,0.75,0.75])

symPts = dict(
	gamma = scl*np.add(org,np.array([bxSdLn*0.5,bxSdLn*0.5,bxSdLn*0.5])),
	L = scl*np.add(org,np.array([bxSdLn*0.5+(a*np.sqrt(2)/2),bxSdLn*0.5+(a*np.sqrt(2)/2),
		bxSdLn*0.5+(a*np.sqrt(2)/2)])),
	K = scl*np.add(org,np.array([bxSdLn-(a*np.sqrt(2)/4),bxSdLn-(a*np.sqrt(2)/4),bxSdLn*0.5])),
	W = scl*np.add(org,np.array([bxSdLn*0.5+(a*np.sqrt(2)/2),bxSdLn,bxSdLn*0.5])),
	X = scl*np.add(org,np.array([bxSdLn*0.5,bxSdLn,bxSdLn*0.5])),
	U = scl*np.add(org,np.array([bxSdLn*0.5+a*np.sqrt(2)/4,bxSdLn,bxSdLn*0.5+a/np.sqrt(8)]))
)

symLnComb = iTt.combinations(list(symPts.keys()),2)
symLns = {}
for i in symLnComb:
	a = np.array([symPts[i[0]], symPts[i[1]]]).T
	symLns[i] = []
	for indx, j in enumerate(a):
		symLns[i].append(np.linspace(j[0], j[1], num = resolutionForPho))
	symLns[i] = np.array(symLns[i])