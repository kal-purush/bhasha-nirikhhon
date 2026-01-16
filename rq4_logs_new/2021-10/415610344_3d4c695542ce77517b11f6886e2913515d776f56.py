import turtle as t


a1=t.Turtle()
a1.speed(100)

a2=t.Turtle()
a2.speed(100)



a1.color("red");
a2.color("blue")

for x in range(1000):
    a1.forward(x)
    a1.left(179.9*((-1)**x))

    a2.forward(-x)
    a2.left(179.9*((-1)**x))
    print(x)