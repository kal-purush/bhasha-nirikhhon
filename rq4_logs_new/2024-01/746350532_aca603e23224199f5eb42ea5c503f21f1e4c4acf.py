from turtle import * 
title('Jai Shree Ram')
bgcolor('black')
pensize(5)
pencolor('orange')
Ram_naam = ["जय श्री राम"] * 50
angle =360/49
penup()
sety(-1)

for i in range(50):
    left(angle)
    forward(260)
    write(Ram_naam[i], align="right",
    font=('Arial',12,"bold"))
    backward(260)

penup()
goto(-40,-20)
pendown()

write(" || जय श्री राम||", align="center", font=("Arial",60
,"normal"))
hideturtle()

done()