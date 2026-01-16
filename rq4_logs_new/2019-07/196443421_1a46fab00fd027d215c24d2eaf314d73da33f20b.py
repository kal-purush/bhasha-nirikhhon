# Lesson 3.3: Use Classes
# Mini-Project: Draw Turtles

# turtle is a library we can use to make simple computer
# graphics. Kunal wants you to try drawing a circles using
# squares. You can also use this space to create other
# kinds of shapes. Experiment and share your results
# on the Discussion Forum!

import turtle

# Your code here.
def draw_square(some_turtle):
    while True:
        for i in range(1,5):
            some_turtle.forward(100)
            some_turtle.right(90)
##            some_turtle.right(60)
##            some_turtle.forward(100)
##            some_turtle.right(120)
        some_turtle.right(6)
    
def draw_art():
    window = turtle.Screen()
    window.bgcolor("grey")

    yu = turtle.Turtle()
    yu.shape("turtle")
    yu.color("white")
    yu.speed(0)
    
    
    draw_square( yu )
    

##    yu1 = turtle.Turtle()
##    yu1.shape("arrow")
##    yu1.color("black")
##    yu1.circle(50)

    window.exitonclick()

draw_art()