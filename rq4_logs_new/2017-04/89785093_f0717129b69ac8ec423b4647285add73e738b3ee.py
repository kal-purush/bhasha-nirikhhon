# -*- coding: utf-8 -*-
"""
Created on Sat Apr 29 11:24:59 2017

@author: shwoo
"""

import math
import random
import numpy as np

datas = []# just a random array of long/lat points.
for i in range(0,20):
    lon = random.randint(146,160)
    lat = random.randint(-39,-30)
    datas.append((lon,lat))
#print datas


def fire_rate_of_spread(V,T,H): #Wind Velocity, Temperature, Humidity
    D = 10  #Drought Factor. 10 is the worst, we choose it 'cause that's a reasonable assumpiton
    W = 15  #fuel density (tons/Ha)
   # t = 0 #inclination in degrees
    F = 2.0*np.exp(-0.45 + 0.987*np.log(D)- 0.0345*H + 0.0338*T + 0.0234*V)
    R = 0.0012*F*W
    #Rt = R*math.exp(0.069*t)
    Z = 13.0*R + 0.24*W - 2.0
    S = R*(4.17 - 0.033*W) -0.36
    return R,Z,S


def getfirefront(lat,lon,distance,bearing):
#    print lat,lon,distance,bearing
  
    R = 6378.1 #Radius of the Earth in km
    brng = bearing #Bearing is 90 degrees converted to radians.
    d = distance #Distance in km

    lat1 = math.radians(lat)
    lon1 = math.radians(lon)
    
    lat2 = math.asin( math.sin(lat1)*math.cos(d/R) +
         math.cos(lat1)*math.sin(d/R)*math.cos(brng))
    
    lon2 = lon1 + math.atan2(math.sin(brng)*math.sin(d/R)*math.cos(lat1),
                 math.cos(d/R)-math.sin(lat1)*math.sin(lat2))
    
    lat2 = math.degrees(lat2)
    lon2 = math.degrees(lon2)
    
    return lat2, lon2

def printa(grid): # A print function for 2D arrays
    for i in grid:
        for j in i:
            print j,
        print "\n"


def mult(grid, C=1):  # multiplies every element in the 2D array by C
    out = grid
    for i in range(0,len(grid)):
        for j in range(0,len(grid[i])):
            out[i][j] = C*grid[i][j]
    return out          


def rotate(grid,theta):#rotates a set of points around the origin.
    rotategrid = grid
    for point in range(0,len(grid)):
        r = np.sqrt(grid[point][0]**2 + grid[point][1]**2)
        x = grid[point][0]
        y = grid[point][1]
                
        t1 = np.arcsin(x/r)      
        if y<0:
            t1 = -np.pi
        t2 = t1+theta
        x2 = r*np.sin(t2)
        y2 = r*np.cos(t2)
        rotategrid[point] = [x2,y2]
        
    return rotategrid



def shapefire(loc1,loc2,theta):#Generates the fire set of coords
    fireshape = [[0,-0.1],[0.1,0],[0.35,0.6],[0.3,0.8],[0.2,0.9],[0,1],[-0.2,0.9],[-0.3,0.8],[-0.35,0.6],[-0.1,0],[0,-0.1]]
        
    latlongscalefactor = np.sqrt((loc1[0]-loc2[0])**2 + (loc1[1]-loc2[1])**2)
    
    fire = rotate(fireshape, theta)
    fire = mult(fire,latlongscalefactor)
    
    for point in range(0,len(fire)):
        fire[point][0] = fire[point][0] + loc1[0]
        fire[point][1] = fire[point][1] + loc1[1]
    return fire

def createAvoid(fire): # creates the avoidance rectangle
    max_x = max(x for x, y in fire)
    max_y = max(y for x, y in fire)
    min_x = min(x for x, y in fire)
    min_y = min(y for x, y in fire)
    
    points = [[max_x,max_y],[min_x,min_y]]
    return points


def fire_constructor(time,ignitionlat,ignitionlon,windspd,theta = 2,temp=30,humidity=0.3):
    theta = np.pi/2 - theta#making 0 to the North.
    rate,flame,spot = fire_rate_of_spread(windspd,temp,humidity)
    
    distance = rate*time
    print distance
    lat2,lon2 = getfirefront(ignitionlat,ignitionlon,distance,theta)
    
    firefront = [lat2,lon2]
    origin = [ignitionlat,ignitionlon]
    
    fire = shapefire(origin,firefront,theta)

    AvoidPoints = createAvoid(fire)
    return AvoidPoints, fire
    
lon = 150.5
lat = -33.7
rad = math.radians(135)
speed = 60

#avoid, fire =  fire_constructor(2,lat,lon,speed,rad,40,0.5)

#print '--boundary--'
#for i in fire:
#    print i[0],',',i[1]
#
#print '--avoid--'
#for i in avoid:
#    print i[0],',',i[1]
