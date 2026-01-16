# ---------------------------- IMPORTS ---------------------------- #
from turtle import Turtle
import random
from playsound import playsound
# ---------------------------- CONSTANTS ---------------------------- #
ALIENS_LOCATION = [(-210, 240), (-140, 240), (-70, 240), (0, 240), (70, 240), (140, 240), (210, 240),
                   (-210, 210), (-140, 210), (-70, 210), (0, 210), (70, 210), (140, 210), (210, 210)]

# ---------------------------- CODE ---------------------------- #
class Aliens(Turtle):
    def __init__(self):
        self.all_aliens = []
        self.score = 0

        # Bullets
        self.all_bullets = []
        self.bullet_speed = 15

    def create_aliens(self):
        for location in ALIENS_LOCATION:
            new_alien = Turtle("turtle")
            new_alien.right(90)
            new_alien.penup()
            new_alien.color("blue")
            new_alien.goto(location)
            self.all_aliens.append(new_alien)

    def create_bullet(self):
        new_bullet = Turtle("circle")
        new_bullet.shapesize(stretch_wid=0.4, stretch_len=0.4)
        new_bullet.penup()
        new_bullet.color("white")
        random_alien = random.choice(self.all_aliens)
        new_bullet.goto(random_alien.xcor(), random_alien.ycor())
        self.all_bullets.append(new_bullet)

    def move_bullet(self):
        for bullet in self.all_bullets:
            bullet_x = bullet.xcor()
            bullet_y = bullet.ycor()
            bullet.goto(bullet_x, bullet_y - self.bullet_speed)

    def get_bullet_position(self):
        for bullet in self.all_bullets:
            return [bullet.xcor, bullet.ycor]

    def remove_alien(self, alien):
        self.all_aliens.remove(alien)
        alien.goto(-5000, 5000)

    def remove_alien_bullet(self, alien_bullet):
        self.all_bullets.remove(alien_bullet)
        alien_bullet.goto(-5000, 10000)

    def remove_all_alien_bullets(self):
        for alien_bullet in self.all_bullets:
            alien_bullet.goto(-5000, 10000)
            self.all_bullets = []

    def detect_hit(self, bullet):
        for alien in self.all_aliens:
            # Quadrant - Right:
            if bullet.xcor() < 0 and alien.xcor() < 0 and bullet.ycor() > 0:
                if abs(abs(bullet.xcor()) - abs(alien.xcor())) < 20:
                    if abs(abs(bullet.ycor()) - abs(alien.ycor())) < 20:
                        self.remove_alien(alien)
                        bullet.goto(5000, 5000)
                        self.score = self.score + 1
                        playsound('sounds/invaderkilled.wav', block=False)
            # Center - Vertical:
            elif bullet.xcor() == 0 and alien.xcor() == 0 and bullet.ycor() > 0:
                if abs(abs(bullet.xcor()) - abs(alien.xcor())) < 20:
                    if abs(abs(bullet.ycor()) - abs(alien.ycor())) < 20:
                        self.remove_alien(alien)
                        bullet.goto(5000, 5000)
                        self.score = self.score + 1
                        playsound('sounds/invaderkilled.wav', block=False)
            # Quadrant - Left:
            elif bullet.xcor() > 0 and alien.xcor() > 0 and bullet.ycor() > 0:
                if abs(abs(bullet.xcor()) - abs(alien.xcor())) < 20:
                    if abs(abs(bullet.ycor()) - abs(alien.ycor())) < 20:
                        self.remove_alien(alien)
                        bullet.goto(5000, 5000)
                        self.score = self.score + 1
                        playsound('sounds/invaderkilled.wav', block=False)

    def number_of_aliens_hit(self):
        return self.score

    def move_right(self):
        for alien in self.all_aliens:
            alien.goto(alien.xcor() + 35, alien.ycor())

    def move_left(self):
        for alien in self.all_aliens:
            alien.goto(alien.xcor() - 35, alien.ycor())
