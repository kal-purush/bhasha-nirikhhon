import turtle 
import random

ninja = turtle.Turtle()

ninja.speed(80)
colors = ['red', 'purple', 'blue', 'green', 'yellow', 'orange'] #Criando uma lista de cores

ninja.pensize(3)
for i in range(120):
    ninja.pencolor(colors[random.randint(0, 5)])
    ninja.forward(70)
    ninja.right(30)
    ninja.forward(15)
    ninja.left(60)
    ninja.forward(40)
    ninja.right(30)
    
    ninja.penup()
    ninja.setposition(0, 0)
    ninja.pendown()
    
    ninja.right(3)
ninja.penup()
ninja.forward(1000)
turtle.done()