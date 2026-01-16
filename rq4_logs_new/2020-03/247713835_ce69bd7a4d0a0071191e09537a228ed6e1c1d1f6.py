#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 15 20:41:53 2020

@author: paul
"""

from pylab import *
import numpy as np

N=20            # Number of squares=N*N

steps=100000000       # number of times to run: if changing this, clear the output box...

boundary=N              # failed at boundary==2 !!!
div=8
figure (figsize=(boundary/div,boundary/div))      # pure aesthetics! div by 9 0r 8

#colors=['b', 'g', 'r', 'c', 'm', 'y', 'k', 'w']
colors=['k','w']        # Black n White!

state_vector=ones([N*N,2])
state_color=np.array(empty([N*N],dtype=str))

ctr=0
for i in range(0,N):
    for j in range(0,N):
        c=np.random.choice(colors)
        line=plot(i,j,c+'s')      # '_s': square markers
        state_vector[ctr,0]=i
        state_vector[ctr,1]=j
        state_color[ctr]=c
        ctr+=1
              
draw()       
savefig("/home/paul/Documents/Output_Stones/Intial_State.png",dpi=400)


def get_color(x,y):
    pos=int(x*N+y)       # Depends on the way this list has been defined...
    return state_color[pos]
    
for i in range(0,steps):
    # choose a random square
    square=np.random.choice([*range(0,ctr)])              # !ctr is not a constant : Careful usage
    x=state_vector[square,0]
    y=state_vector[square,1]
    
    dx=np.random.choice([0,1,-1])
    dy=np.random.choice([0,1,-1])
    x=x+dx
    y=y+dy
    
    if (x>=N):
        x-=N
    if(x<0):
        x+=N
    if(y>=N):
        y-=N
    if(y<0):
        y+=N
        
    state_color[square]=get_color(x,y)
    
    if(i<20 or i==steps-1 or i in [*range(0,steps,int(steps/20))] ) :       # 1st 20 steps then 20 other steps... For large steps otherwise remove this if...
        #update color
        line=plot(state_vector[square,0],state_vector[square,1],state_color[square]+'s')
        draw()
        savefig("/home/paul/Documents/Output_Stones/"+str(i)+".png",dpi=400)


    
    # The new overlapping squares are pretty chaotic, but I guess that's okayy...
    # plot out of the loop, if you are interested in only the final process; speeds up!     done something for this!!!


