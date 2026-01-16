import turtle
from turtle import *
wn = Screen()
#background colour black
wn.bgcolor('black')
t = turtle.Turtle()
t.pencolor('white')
def curve():
    for i in range(200):
        t.right(1)
        t.forward(1)
        
def heart():
    t.fillcolor('red')
    t.begin_fill()
    t.left(140)
    t.forward(113)
    curve()
    t.left(120)
    curve()
    t.forward(112)
    t.end_fill()
heart()
t.hideturtle()
def write(message,pos):
    x,y=pos
    t.penup()
    t.goto(x,y)
    t.color('white')
    style=('Stencil Std',18,'italic')
    t.write(message, font=style)
write('I',(-68,95))
write('L',(-55,95))
write('O',(-42,95))
write('V',(-25,95))
write('E',(-10,95))
write('Y',(15,95))
write('O',(30,95))
write('U',(50,95))
turtle.done()