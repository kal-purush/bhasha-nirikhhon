import sys
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.mlab as mlab
from mpl_toolkits.mplot3d import Axes3D
import math

measurements = []

def init():# dynamix matrix
	readData()

def readData():
	global measurements

	#path = './../RaspberryPi-CarPC/TinkerDataLogger/DataLogs/2014/'
	datafile = '../data/2014-03-26-000-Data.csv'

	date, \
	time, \
	millis, \
	ax, \
	ay, \
	az, \
	rollrate, \
	pitchrate, \
	yawrate, \
	roll, \
	pitch, \
	yaw, \
	speed, \
	course, \
	latitude, \
	longitude, \
	altitude, \
	pdop, \
	hdop, \
	vdop, \
	epe, \
	fix, \
	satellites_view, \
	satellites_used, \
	temp = np.loadtxt(datafile, delimiter=',', unpack=True,skiprows=1)

	print('Read \'%s\' successfully.' % datafile)

	# A course of 0 Deg means the Car is traveling north bound
	# and 90 Deg means it is traveling east bound.
	# In the Calculation following, East is Zero and North is 90 Deg
	# We need an offset.
	course =(-course+90.0)
	dt = np.hstack([0.02, np.diff(millis)])/1000.0 # in s

	RadiusEarth = 6378388.0 # m
	arc= 2.0*np.pi*(RadiusEarth+altitude)/360.0 # m/Deg

	dx = arc * np.cos(latitude*np.pi/180.0) * np.hstack((0.0, np.diff(longitude))) # in m
	dy = arc * np.hstack((0.0, np.diff(latitude))) # in m

	mx = np.cumsum(dx)
	my = np.cumsum(dy)

	ds = np.sqrt(dx**2+dy**2)

	GPS=(ds!=0.0).astype('bool') # GPS Trigger for Kalman Filter
	measurements = np.vstack((mx, my))

	x = np.matrix([[mx[0], my[0], 0.5*np.pi, 0.0]]).T # initial state

	# numstates = 4
	# gs = np.matrix([[xs+vs*dts*cos(psis)],
 #             		[ys+vs*dts*sin(psis)],
 #             		[psis],
 #             		[vs]])

	# state = np.matrix([[1.0, 1.0, 1.0, 1.0]]).T
	# P = np.eye(numstates) * 1000.0

	# JHs = np.matrix([[1.0, 0.0, 0.0, 0.0],
	# 	             [0.0, 1.0, 0.0, 0.0]])

	# varGps = 6.0
	# R = np.diag([varGps**2, varGps**2])

	# I = np.eye(numstates)

def plotPath():
	global measurements

	plt.plot(measurements[0], measurements[1])
	plt.show()

init()
plotPath()