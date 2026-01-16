#Turtle Party Project 
#by Emily Hill
# 03/18/24
import turtle

def rectangle(width,height):
   for i in range(2):
     turtle.forward(width)
     turtle.right(90)
     turtle.forward(height)
     turtle.right(90)
     
     
def Octagon(len):
  for i in range(8):
    turtle.forward(len)
    turtle.left(360/8)   

def stop(len):
  Octagon(len)
  turtle.forward(len*(3/8))
  rectangle(len*(1/5),len*2)

def back(len):
  turtle.penup()
  turtle.backward(len)
  turtle.pendown()
  
  
turtle.color("blue")
stop(60)
back(150)
turtle.color("red")
stop(40)
     
     