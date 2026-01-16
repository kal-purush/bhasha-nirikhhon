from turtle import Turtle, Screen

timmy = Turtle() # assigns timmy the class name which makes timmy an object. The paranethis at the end of class creats the object

my_screen = Screen()
my_screen.setup(width=500,height=500,startx=None,starty=None)
timmy.shape("turtle")
timmy.color("red","green")


timmy.forward(100)
timmy.left(45)
timmy.forward(25)
timmy.left(45)
timmy.forward(25)


my_screen.exitonclick()