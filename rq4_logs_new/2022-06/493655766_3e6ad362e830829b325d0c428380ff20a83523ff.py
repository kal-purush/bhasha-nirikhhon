import turtle
import os

# creating the screen / window
win = turtle.Screen()
win.title("Yehan Yeshminda PongGame!")
win.bgcolor('black')
win.setup(width=800, height=600)  # setting the width and the height of the screen window

win.tracer(0)  # Stops the window from updating and speeds up the game

# PADDLE A
paddle_a = turtle.Turtle()
paddle_a.speed(0)  # to speed things up not the speed of the paddle
paddle_a.shape("square")
paddle_a.color("white")
paddle_a.shapesize(stretch_wid=5, stretch_len=1) #  default is 20 = 5 x 20 in width and 1 x 20 in height
paddle_a.penup()  # by default since turtle draws a line we make sure that a line is now drawn using this code
paddle_a.goto(-350, 0)  # Start at X Coors = -350 and Y = 0


# PADDLE B
paddle_b = turtle.Turtle()
paddle_b.speed(0)  # to speed things up not the speed of the paddle
paddle_b.shape("square")
paddle_b.color("white")
paddle_b.shapesize(stretch_wid=5, stretch_len=1) #  default is 20 = 5 x 20 in width and 1 x 20 in height
paddle_b.penup()  # by default since turtle draws a line we make sure that a line is now drawn using this code
paddle_b.goto(350, 0)  # Start at X Coors = -350 and Y = 0


# CREATING THE BALL
ball = turtle.Turtle()
ball.speed(0)  # to speed things up not the speed of the paddle
ball.shape("circle")
ball.color("white")
ball.penup()  # by default since turtle draws a line we make sure that a line is now drawn using this code
ball.goto(0, 0)  # Start at X Coors = -350 and Y = 0

# SETTING THE BALL MOVEMENT
ball.dx = 0.1
ball.dy = 0.1

# FUNCTIONS TO MOVE FROM THE KEYWORD
def paddle_y_up():
    y = paddle_a.ycor()  # to get the coords of the paddle
    y += 20  # to add 20 pixels to the paddle and move up
    paddle_a.sety(y)  # to change the y axis to the current y axis set with the y with the new variable

def paddle_y_down():
    y = paddle_a.ycor()  # to get the coordinates of the paddle
    y -= 20  # to add 20 pixels to the paddle and move up
    paddle_a.sety(y)  # to change the y-axis to the current y-axis set with the y with the new variable

# FUNCTIONS TO MOVE FROM THE KEYWORD
def paddle_x_up():
    y = paddle_b.ycor()  # to get the coords of the paddle
    y += 20  # to add 20 pixels to the paddle and move up
    paddle_b.sety(y)  # to change the y axis to the current y axis set with the y with the new variable


def paddle_x_down():
    y = paddle_b.ycor()  # to get the coordinates of the paddle
    y -= 20  # to add 20 pixels to the paddle and move up
    paddle_b.sety(y)  # to change the y-axis to the current y-axis set with the y with the new variable

# Listens until something is pressed
win.listen()
win.onkeypress(paddle_y_up, "w")  # waits for a keypress of w and when pressed it executes the function
win.onkeypress(paddle_y_down, "s")  # waits for a keypress of w and when pressed it executes the function
win.onkeypress(paddle_x_up, "Up")
win.onkeypress(paddle_x_down, "Down")

# CREATING THE MAIN WINDOW
while True:
    win.update()

    # MOVE THE BALL
    ball.setx(ball.xcor() + ball.dx)
    ball.sety(ball.ycor() + ball.dy)

    if ball.ycor() > 290:
        ball.sety(290)
        ball.dy *= -1

    if ball.ycor() < -290:
        ball.sety(-290)
        ball.dy *= -1

    if ball.xcor() > 390:
        ball.goto(0,0)
        ball.dx *= -1

    if ball.xcor() < -390:
        ball.goto(0,0)
        ball.dx *= -1

