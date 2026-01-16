#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @file: multple_ball_rectangular_table.py
# @author: Lucas Martins 20065236
# @brief: Multiple ball bouncing around inside a rectangular table
# @project: python pool table
from __future__ import print_function, division
from visual import *
import random

# constants
radius = 1 # ball radius and standard unit length for simulation
width, height = 60*radius, 40*radius

table = box(pos=(0,0,-radius-0.1), size=(width,height,0.2), color=(.2,.5,.1))
wallNorth = box(pos=(0,height/2+radius/2,-radius/2-0.1), size=(width,radius,radius+0.2), color=(.4,.2,0))
wallSouth = box(pos=(0,-height/2-radius/2,-radius/2-0.1), size=(width,radius,radius+0.2), color=(.4,.2,0))
wallWest = box(pos=(-width/2-radius/2,0,-radius/2-0.1), size=(radius,height+2*radius,radius+0.2), color=(.4,.2,0))
wallEast = box(pos=(+width/2+radius/2,0,-radius/2-0.1), size=(radius,height+2*radius,radius+0.2), color=(.4,.2,0))

balls = []

cueBall = sphere(radius=radius, velocity=vector(5,0,0), pos=(-width*.35,0,0), color=color.white) 
balls.append(cueBall)

posy = 1
for x in range(5):
    for y in range(x):
        ball = sphere(radius=radius, velocity=vector(0,0,0), pos=(1.5*x,2*y+posy,0), color=color.red) 
        balls.append(ball)
    posy -= 1

# callbacks
def keydown(evt):
    s = evt.key

    if s in ['esc', 'q', 'Q']:exit()

scene.bind('keydown',keydown)

dt = 0.1
while true:
    rate(100)
    for ball in balls:
        ball.pos += dt * ball.velocity

        if ball.x < (-width/2 + 2*radius): # left wall
            ball.x = -width/2 + 2*radius
            ball.velocity.x = -ball.velocity.x
        if ball.y > (height/2 - 2*radius):  # top wall
            ball.y = height/2 - 2*radius
            ball.velocity.y = -ball.velocity.y
        if ball.x > (width/2 - 2*radius):
            ball.x = width/2 - 2*radius
            ball.velocity.x = -ball.velocity.x
        if ball.y < (-height/2 + 2*radius): # bottom wall
            ball.y = -height/2 + 2*radius
            ball.velocity.y = -ball.velocity.y
            
        for otherBall in balls:
            if otherBall==ball: break

            d = otherBall.pos - ball.pos    # vector between ball positions
            if mag(d)>2*radius: continue    # no collision if distance is greater than 2*radius

            relativeVelocity = ball.velocity - otherBall.velocity
            normal = norm(d)
            closingSpeed = dot(normal, relativeVelocity)

            if closingSpeed<0: continue     # ignore if balls are moving apart
            ball.velocity -= closingSpeed*normal
            otherBall.velocity += closingSpeed*normal