import turtle as trtl
import random as rand


#creating turtles
apple_image = "apple5.gif"
apple = trtl.Turtle()
snake = trtl.Turtle()
wn = trtl.Screen()
counter = trtl.Turtle()
#set up timer
counter.penup()
counter.goto(340,360 )
counter.hideturtle()
#Countdown Var
font_setup = ("Arial", 20, "normal")
timer = 60
counter_interval = 1000
timer_up = False
#Timer
def countdown():
 global timer, timer_up
 counter.clear()
 if timer <= 0:
   counter.write("Time's Up", font=font_setup)
   timer_up = True
 else:
   counter.write("Timer: " + str(timer), font=font_setup)
   timer -= 1
   counter.getscreen().ontimer(countdown, counter_interval)
#set up snake and turtle
wn.addshape(apple_image)
apple.shape(apple_image)
apple.hideturtle()
apple.penup()
apple.goto(50,0)
apple.showturtle()
wn.bgcolor("black")
snake.color('green')
snake.penup()
speed = .3
def travel():
   snake.forward(speed)
   wn.ontimer(travel, 1)
   touch()
wn.onkey(lambda: snake.setheading(90), 'Up')
wn.onkey(lambda: snake.setheading(180), 'Left')
wn.onkey(lambda: snake.setheading(0), 'Right')
wn.onkey(lambda: snake.setheading(270), 'Down')


def randomapple():
   apple.penup()
   possible = [-300, -200, -100, -25, 25, 100, 200, 300]
   locationx = rand.choice(possible)
   locationy = rand.choice(possible)
   apple.hideturtle()
   apple.goto(locationx, locationy)
   apple.showturtle()
def touch():
    if abs(apple.xcor() - snake.xcor()) < 5:
       if abs(apple.ycor() - snake.ycor()) < 5:
           global speed
           randomapple()
           speed += .1

wn.listen()
travel()
randomapple()
wn.ontimer(countdown, counter_interval)
wn = trtl.Screen()
wn.bgpic("snakeBg4.png")
wn.mainloop()