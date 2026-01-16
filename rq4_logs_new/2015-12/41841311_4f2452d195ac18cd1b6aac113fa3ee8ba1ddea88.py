#how to draw a square in python

import turtle

def draw_square():
    # create a canvas for the square first
    window = turtle.Screen()
    # set the background color to red
    window.bgcolor("red")


    # to activate the turtle module, and use the Class Turtle
    brad = turtle.Turtle()
    # customize the shape and color and speed of the turtle
    brad.shape("classic")
    brad.color("green")
    brad.speed(2)

    # draw individual lines of the square and how long they each should travel
    brad.forward(100)
    brad.right(90)
    brad.forward(100)
    brad.right(90)
    brad.forward(100)
    brad.right(90)
    brad.forward(100)
    brad.right(90)

    # draw a circle
    angie = turtle.Turtle()
    angie.shape("arrow")
    angie.color("green")
    angie.circle(100)

    # draw a triangle
    trevor = turtle.Turtle()
    trevor.shape("arrow")
    trevor.color("green")
    
    trevor.right(120)
    trevor.forward(100)
    trevor.right(120)
    trevor.forward(100)
    trevor.right(120)
    trevor.forward(100)
    
    

    # set the windown to exit when you click on it
    window.exitonclick()
draw_square()
    