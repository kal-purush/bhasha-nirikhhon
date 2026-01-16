#Bounce.py
#Josh Bowen
#4/8/2021

import turtle
import random

wn = turtle.Screen()
wn.bgcolor("Black")

bounce = turtle.Turtle()
bounce.color("lightgreen")
bounce.speed(0)
bounce.shape("circle")

#In the future I want to make any bounds and make it go somewhere and record
#where it goes and make that a part of the while loop later on...you would need
# a lot of and's and items from the list of postitions turtle.bound
bound = turtle.Turtle()
bound.color("White")
bound.speed(0)
bound.width(10)
bound.ht()

def perfect_turn():
    # need to make a function that takes bounce.heading and some how finds
    # the angle between it and the bounds and then returns the amount needed
    # to make an acurate bounce... for now we will just increment by 10 
    return 10


bound.up()
bound.goto(-200,-200)
bound.down()
for i in range(4):
    bound.fd(400)
    bound.lt(90)

bounce.up()
bounce.fd(random.randint(-200,200))
bounce.down()
bounce.lt(random.randrange(0,360))
go = "yes"

while go == "yes":
    if bounce.xcor() < 200 and bounce.xcor() > -200 and bounce.ycor() < 200 and bounce.ycor()  > -200:
        bounce.fd(1)
    elif bounce.xcor() < 200 or bounce.xcor() > -200 or bounce.ycor() < 200 or bounce.ycor()  > -200:
        bounce.right(perfect_turn()) ##this needs to choose right or left somehow
        bounce.fd(1)