import random
from turtle import Turtle

COLORS = ["red", "orange", "yellow", "green", "blue", "purple"]
STARTING_MOVE_DISTANCE = 5
MOVE_INCREMENT = 10


class CarManager(Turtle):
    cars = []

    def __init__(self):
        super().__init__()
        self.move_speed = STARTING_MOVE_DISTANCE
        self.hideturtle()

    def starting_cars(self):
        for num in range(0, 20):
            car = Turtle()
            car.shape("square")
            car.color(random.choice(COLORS))
            car.penup()
            car.setheading(180)
            car.shapesize(stretch_len=2, stretch_wid=1)
            car.goto(random.randint(-300, 250), random.randint(-240, 280))
            self.cars.append(car)

    def new_car(self):
        car = Turtle()
        car.shape("square")
        car.color(random.choice(COLORS))
        car.penup()
        car.setheading(180)
        car.shapesize(stretch_len=2, stretch_wid=1)
        car.goto(300, random.randint(-240, 280))
        self.cars.append(car)

    def speed_up(self):
        self.move_speed += MOVE_INCREMENT

    def clear_cars(self):
        for car in self.cars:
            car.ht()
        self.cars.clear()

    def move(self, player_location):
        for car in self.cars:
            car.forward(self.move_speed)
            if car.distance(player_location) < 20:
                return False
        return True