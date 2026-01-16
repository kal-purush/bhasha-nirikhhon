import tkinter as tk
import random



#==========================================
# Purpose: This class represents the setup of the game. It creates the window, the board, and each character of the game
# Instance variables: self.win: Window in which the game is played
#                     self.canvas: Creates the canvas that holds all the game objects
#                     self.speed: Controls the speed of the game according to the chosen difficulty
#                     self.old_snake: bool, represents whether the snake of the previous round died or not when reset
#                     self.paused: bool, stoes whether the game is paused or not
#                     self.x: top-left x-coordinate of the user-controlled snake
#                     self.y: top-left y-coordinate of the user-controlled snake
#                     self.enemy_x: top-left x-coordinate of the computer-controlled snake
#                     self.enemy_y: top-left y-coordinate of the computer-controlled snake
#                     self.snake: Object of Snake class. Represents the snake that the user controls
#                     self.enemy: Object of Enemy class. Represents the snake that the computer controls
#                     self.oval_x: int, x-coordinate of the food that randomly spawns on the board
#                     self.oval_y:int, y-coordinate of the food that randomly spawns on the board
#                     self.pellet: Circle created on screen to represent the food the snake has to go for
# Methods: __init__: Does the setup for the game that only needs to happen once such as the key bindings and window setup
#          init2: Does the setup that happens each time the game gets reset, deletes all elements of game and replaces them with defaults
#          pause: Stops the game when the spacebar is pressed, and resumes it when spacebar is pressed again
#          reset: Calls the init2 method, resets the game to the beginning when the 'r' key is pressed
#          speed_easy: Sets the game to move the snakes every 200ms
#          speed_medium: Sets the game to move the snakes every 100ms
#          speed_hard: Sets the game to move the snakes every 50ms
#          snake_move: Moves the user-controlled snake according to its current velocity
#          enemy_move: Moves the computer-controlled snake according to its current velocity
#          new_pellet: Deletes the current food pellet and creates a new one in a random location in the board
#          gameloop: Calls all the functions related to gameplay and moves the snakes every interval of the self.speed
#==========================================

class SnakeGUI:
    #One-time setup    
    def __init__(self):
        self.win = tk.Tk()
        self.canvas = tk.Canvas(self.win, height=660, width=660)
        self.canvas.pack()
        self.speed = 100
        self.old_snake = True
        self.init2()
        self.win.bind('r',self.reset)
        self.win.bind(1, self.speed_easy)
        self.win.bind(2, self.speed_medium)
        self.win.bind(3, self.speed_hard)
        self.paused = False
    #Sets game characters and resets
    def init2(self):
        self.paused = False
        self.canvas.delete(tk.ALL)
        self.board = self.canvas.create_rectangle(30, 30, 630, 630)
        self.x = 330
        self.y = 330
        self.enemy_x = 30
        self.enemy_y = 30
        self.snake = Snake(self.x, self.y, 'green', self.canvas)
        self.enemy = Enemy(self.enemy_x, self.enemy_y, 'purple', self.canvas)
        self.oval_x = 30 + (30 * random.randint(0, 19))
        self.oval_y = 30 + (30 * random.randint(0, 19))
        self.pellet = self.canvas.create_oval(self.oval_x, self.oval_y, self.oval_x + 30, self.oval_y + 30, fill='blue')
        self.win.bind('<Down>',self.snake.go_down)
        self.win.bind('<Left>',self.snake.go_left)
        self.win.bind('<Right>',self.snake.go_right)
        self.win.bind('<Up>',self.snake.go_up)
        self.win.bind('<space>', self.pause)
        if self.old_snake:
            self.gameloop()
    def pause(self, event):
        self.paused = not self.paused
        if not self.paused:
            self.canvas.delete(self.pause_label)
            self.gameloop()         
    #Called when game resets
    def reset(self, event):
        self.old_snake = self.snake.collide
        self.init2()
    def speed_easy(self, event):
        self.old_snake = self.snake.collide
        self.speed = 200
        self.init2()
    def speed_medium(self, event):
        self.old_snake = self.snake.collide
        self.speed = 100
        self.init2()
    def speed_hard(self, event):
        self.old_snake = self.snake.collide
        self.speed = 50
        self.init2()
    #Controls how the snake moves
    def snake_move(self):
        self.x += (self.snake.vx)
        self.y += (self.snake.vy)
        self.snake.move(self.x, self.y, self.oval_x, self.oval_y, self.canvas)
    #Controls how the enemy moves
    def enemy_move(self):

        #Helps snake go to the food           
        if self.enemy_y < self.oval_y:
            self.enemy.go_down()
        elif self.enemy_y > self.oval_y:
            self.enemy.go_up()         
        elif self.enemy_x < self.oval_x:
            self.enemy.go_right()        
        elif self.enemy_x > self.oval_x:
            self.enemy.go_left()

        #Handles if snake is on the edge of the board
        if self.enemy_x == 570 or self.enemy_x == 30:
            if self.enemy_y < self.oval_y:
                self.enemy.go_down()
            elif self.enemy_y > self.oval_y:
                self.enemy.go_up()
            elif self.enemy_y == self.oval_y and self.enemy_x == 570:
                self.enemy.go_left()
            elif self.enemy_y == self.oval_y and self.enemy_y == 30:
                self.enemy.go_right()
        if self.enemy_y == 30 or self.enemy_y == 570:
            if self.enemy_x < self.oval_x:
                self.enemy.go_right()  
            elif self.enemy_x > self.oval_x:
                self.enemy.go_left()
            elif self.enemy_x == self.oval_x and self.enemy_y == 570:
                self.enemy.go_up()
            elif self.enemy_x == self.oval_x and self.enemy_y == 30:
                self.enemy.go_down()

        #Keeps it moving in same direction
        self.enemy_x += self.enemy.vx
        self.enemy_y += self.enemy.vy
        
        if self.enemy.collide:
            for seg in self.enemy.segments:
                self.canvas.delete(seg)
            return
    
        #Actually moves enemy based on above conditions
        self.enemy.move(self.enemy_x, self.enemy_y, self.oval_x, self.oval_y, self.canvas)

    #Creates new pellet in random location      
    def new_pellet(self):
        self.canvas.delete(self.pellet)
        self.oval_x = 30 + (30 * random.randint(0, 19))
        self.oval_y = 30 + (30 * random.randint(0, 19))
        self.pellet = self.canvas.create_oval(self.oval_x, self.oval_y, self.oval_x + 30, self.oval_y + 30, fill='blue')

    #Runs all functions needed for game and checks if snake collided
    def gameloop(self):
        if self.paused:
            self.pause_label = self.canvas.create_text(330, 330, fill="black", text="PAUSED. Press SPACE to Resume")
            return
        self.snake_move()
        
        self.enemy_move()
             
        if self.x == self.oval_x and self.y == self.oval_y:
            self.new_pellet()
        if self.enemy_x == self.oval_x and self.enemy_y == self.oval_y:
            self.new_pellet()
            
        #Checks for collision
        for s_seg in self.snake.segments:
            for e_seg in self.enemy.segments:
                if self.canvas.coords(s_seg) == self.canvas.coords(e_seg):
                    self.snake.collide = True
                    self.enemy.collide = True

        if not(self.snake.collide):
            self.canvas.after(self.speed, self.gameloop)
        else:
            self.snake.print_lose()

#==========================================
# Purpose: Represents the user-controlled snake and the functions associated with it
# Instance variables: self.x: x-coordinate of the user-controlled snake
#                     self.y: y-coordinate of the user-controlled snake
#                     self.color: color of the snake
#                     self.canvas: The canvas in which this snake will operate
#                     self.segments: List containing all the segments of the snake's body
#                     self.vx: The x-velocity of the snake
#                     self.vy: the y-velocity of the snake
#                     self.collide: bool representing whether the snake already crashed or not
# Methods: __init__(self, x, y, color, canvas): Sets each of the instance variables of this class to their default values
#          print_lose(self): Creates a label in the screen telling the user they lost and showing their score
#          move(self, x, y, x_pellet, y_pellet, canvas): Controls how the snake moves, which is by making a new segment in front and removing the one from the back
#          go_down(self, event): Changes the y-velocity to 30 and the x-velocity to 0
#          go_up(self, event): Changes the y-velocity to -30 and the x-velocity to 0
#          go_right(self, event): Changes the x-velocity to 30 and the y-velocity to 0
#          go_down(self, event): Changes the x-velocity to -30 and the y-velocity to 0
#==========================================        
class Snake:
    def __init__(self, x, y, color, canvas):
        self.x = x
        self.y = y
        self.color = color
        self.canvas = canvas
        self.segments = []
        self.vx = 30
        self.vy = 0
        self.collide = False
        self.segments.insert(0, canvas.create_rectangle(x, y, x+30, y+30, fill=color))
    def print_lose(self):
        self.canvas.create_text(330, 330, fill="black", text="You Lose! Score= " + str(len(self.segments)))     
    def move(self, x, y, x_pellet, y_pellet, canvas):
        self.collide = False
        
        #Adds to snake if it eats the food
        if x == x_pellet and y == y_pellet:
            self.segments.insert(0, canvas.create_rectangle(x, y, x+30, y+30, fill=self.color))

        #Ends game if snake hits edge of board    
        if canvas.coords(self.segments[0])[0] < 30 or canvas.coords(self.segments[0])[1] < 30 or canvas.coords(self.segments[0])[2] > 630 or canvas.coords(self.segments[0])[3] > 630:
            self.collide = True
            return
        #Adds one in front and removes the last, how normal movement works
        self.segments.insert(0, canvas.create_rectangle(x, y, x+30, y+30, fill=self.color))
        self.canvas.delete(self.segments[-1])
        self.segments.pop()
        
        #Checks if snake hits itself
        for i in range(2, len(self.segments)):
            if self.canvas.coords(self.segments[0]) == self.canvas.coords(self.segments[i]):
                self.collide = True
                return
    def go_down(self, event):
        if self.vy != -30:
            self.vx = 0
            self.vy = 30
    def go_right(self, event):
        if self.vx != -30:
            self.vx = 30
            self.vy = 0
    def go_up(self, event):
        if self.vy != 30:
            self.vx = 0
            self.vy = -30
    def go_left(self, event):
        if self.vx != 30:
            self.vx = -30
            self.vy = 0

#==========================================
# Purpose: Instance of this class represents an enemy snake
# Instance variables: All methods inherited from Snake class
#                   self.vx: x-component of the enemy snake's velocity
#                   self.vy: y-component of the enemy snake's velocity
# Methods: __init(self, x, y, color, canvas): Inherits init variables from Snake class
#          go_down(self): Changes the snake's velocity so it goes down
#          go_up(self): Changes the snake's velocity so it goes up
#          go_right(self): Changes the snake's velocity so it goes right
#          go_left(self): Changes the snake's velocity so it goes left
#==========================================
class Enemy(Snake):
    def __init__(self, x, y, color, canvas):
        Snake.__init__(self, x, y, color, canvas)
    def go_down(self):
        if self.vy != -30:
            self.vx = 0
            self.vy = 30
    def go_right(self):
        if self.vx != -30:
            self.vx = 30
            self.vy = 0
    def go_up(self):
        if self.vy != 30:
            self.vx = 0
            self.vy = -30
    def go_left(self):
        if self.vx != 30:
            self.vx = -30
            self.vy = 0
    
        
SnakeGUI()
tk.mainloop()