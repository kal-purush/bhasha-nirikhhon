import turtle, time, random

colors = ['snow', 'ghost white', 'white smoke', 'gainsboro', 'floral white', 'old lace',
    'linen', 'antique white', 'papaya whip', 'blanched almond', 'bisque', 'peach puff',
    'navajo white', 'lemon chiffon', 'mint cream', 'azure', 'alice blue', 'lavender',
    'lavender blush', 'misty rose', 'dark slate gray', 'dim gray', 'slate gray',
    'light slate gray', 'gray', 'light grey', 'midnight blue', 'navy', 'cornflower blue', 'dark slate blue',
    'slate blue', 'medium slate blue', 'light slate blue', 'medium blue', 'royal blue',  'blue',
    'dodger blue', 'deep sky blue', 'sky blue', 'light sky blue', 'steel blue', 'light steel blue',
    'light blue', 'powder blue', 'pale turquoise', 'dark turquoise', 'medium turquoise', 'turquoise',
    'cyan', 'light cyan', 'cadet blue', 'medium aquamarine', 'aquamarine', 'dark green', 'dark olive green',
    'dark sea green', 'sea green', 'medium sea green', 'light sea green', 'pale green', 'spring green',
    'lawn green', 'medium spring green', 'green yellow', 'lime green', 'yellow green',
    'forest green', 'olive drab', 'dark khaki', 'khaki', 'pale goldenrod', 'light goldenrod yellow',
    'light yellow', 'yellow', 'gold', 'light goldenrod', 'goldenrod', 'dark goldenrod', 'rosy brown',
    'indian red', 'saddle brown', 'sandy brown',
    'dark salmon', 'salmon', 'light salmon', 'orange', 'dark orange',
    'coral', 'light coral', 'tomato', 'orange red', 'red', 'hot pink', 'deep pink', 'pink', 'light pink',
    'pale violet red', 'maroon', 'medium violet red', 'violet red',
    'medium orchid', 'dark orchid', 'dark violet', 'blue violet', 'purple', 'medium purple',
    'thistle', 'snow2', 'snow3',
    'snow4', 'seashell2', 'seashell3', 'seashell4', 'AntiqueWhite1', 'AntiqueWhite2',
    'AntiqueWhite3', 'AntiqueWhite4', 'bisque2', 'bisque3', 'bisque4', 'PeachPuff2',
    'PeachPuff3', 'PeachPuff4', 'NavajoWhite2', 'NavajoWhite3', 'NavajoWhite4',
    'LemonChiffon2', 'LemonChiffon3', 'LemonChiffon4', 'cornsilk2', 'cornsilk3',
    'cornsilk4', 'ivory2', 'ivory3', 'ivory4', 'honeydew2', 'honeydew3', 'honeydew4',
    'LavenderBlush2', 'LavenderBlush3', 'LavenderBlush4', 'MistyRose2', 'MistyRose3',
    'MistyRose4', 'azure2', 'azure3', 'azure4', 'SlateBlue1', 'SlateBlue2', 'SlateBlue3',
    'SlateBlue4', 'RoyalBlue1', 'RoyalBlue2', 'RoyalBlue3', 'RoyalBlue4', 'blue2', 'blue4',
    'DodgerBlue2', 'DodgerBlue3', 'DodgerBlue4', 'SteelBlue1', 'SteelBlue2',
    'SteelBlue3', 'SteelBlue4', 'DeepSkyBlue2', 'DeepSkyBlue3', 'DeepSkyBlue4',
    'SkyBlue1', 'SkyBlue2', 'SkyBlue3', 'SkyBlue4', 'LightSkyBlue1', 'LightSkyBlue2',
    'LightSkyBlue3', 'LightSkyBlue4', 'SlateGray1', 'SlateGray2', 'SlateGray3',
    'SlateGray4', 'LightSteelBlue1', 'LightSteelBlue2', 'LightSteelBlue3',
    'LightSteelBlue4', 'LightBlue1', 'LightBlue2', 'LightBlue3', 'LightBlue4',
    'LightCyan2', 'LightCyan3', 'LightCyan4', 'PaleTurquoise1', 'PaleTurquoise2',
    'PaleTurquoise3', 'PaleTurquoise4', 'CadetBlue1', 'CadetBlue2', 'CadetBlue3',
    'CadetBlue4', 'turquoise1', 'turquoise2', 'turquoise3', 'turquoise4', 'cyan2', 'cyan3',
    'cyan4', 'DarkSlateGray1', 'DarkSlateGray2', 'DarkSlateGray3', 'DarkSlateGray4',
    'aquamarine2', 'aquamarine4', 'DarkSeaGreen1', 'DarkSeaGreen2', 'DarkSeaGreen3',
    'DarkSeaGreen4', 'SeaGreen1', 'SeaGreen2', 'SeaGreen3', 'PaleGreen1', 'PaleGreen2',
    'PaleGreen3', 'PaleGreen4', 'SpringGreen2', 'SpringGreen3', 'SpringGreen4',
    'green2', 'green3', 'green4', 'chartreuse2', 'chartreuse3', 'chartreuse4',
    'OliveDrab1', 'OliveDrab2', 'OliveDrab4', 'DarkOliveGreen1', 'DarkOliveGreen2',
    'DarkOliveGreen3', 'DarkOliveGreen4', 'khaki1', 'khaki2', 'khaki3', 'khaki4',
    'LightGoldenrod1', 'LightGoldenrod2', 'LightGoldenrod3', 'LightGoldenrod4',
    'LightYellow2', 'LightYellow3', 'LightYellow4', 'yellow2', 'yellow3', 'yellow4',
    'gold2', 'gold3', 'gold4', 'goldenrod1', 'goldenrod2', 'goldenrod3', 'goldenrod4',
    'DarkGoldenrod1', 'DarkGoldenrod2', 'DarkGoldenrod3', 'DarkGoldenrod4',
    'RosyBrown1', 'RosyBrown2', 'RosyBrown3', 'RosyBrown4', 'IndianRed1', 'IndianRed2',
    'IndianRed3', 'IndianRed4', 'sienna1', 'sienna2', 'sienna3', 'sienna4', 'burlywood1',
    'burlywood2', 'burlywood3', 'burlywood4', 'wheat1', 'wheat2', 'wheat3', 'wheat4', 'tan1',
    'tan2', 'tan4', 'chocolate1', 'chocolate2', 'chocolate3', 'firebrick1', 'firebrick2',
    'firebrick3', 'firebrick4', 'brown1', 'brown2', 'brown3', 'brown4', 'salmon1', 'salmon2',
    'salmon3', 'salmon4', 'LightSalmon2', 'LightSalmon3', 'LightSalmon4', 'orange2',
    'orange3', 'orange4', 'DarkOrange1', 'DarkOrange2', 'DarkOrange3', 'DarkOrange4',
    'coral1', 'coral2', 'coral3', 'coral4', 'tomato2', 'tomato3', 'tomato4', 'OrangeRed2',
    'OrangeRed3', 'OrangeRed4', 'red2', 'red3', 'red4', 'DeepPink2', 'DeepPink3', 'DeepPink4',
    'HotPink1', 'HotPink2', 'HotPink3', 'HotPink4', 'pink1', 'pink2', 'pink3', 'pink4',
    'LightPink1', 'LightPink2', 'LightPink3', 'LightPink4', 'PaleVioletRed1',
    'PaleVioletRed2', 'PaleVioletRed3', 'PaleVioletRed4', 'maroon1', 'maroon2',
    'maroon3', 'maroon4', 'VioletRed1', 'VioletRed2', 'VioletRed3', 'VioletRed4',
    'magenta2', 'magenta3', 'magenta4', 'orchid1', 'orchid2', 'orchid3', 'orchid4', 'plum1',
    'plum2', 'plum3', 'plum4', 'MediumOrchid1', 'MediumOrchid2', 'MediumOrchid3',
    'MediumOrchid4', 'DarkOrchid1', 'DarkOrchid2', 'DarkOrchid3', 'DarkOrchid4',
    'purple1', 'purple2', 'purple3', 'purple4', 'MediumPurple1', 'MediumPurple2',
    'MediumPurple3', 'MediumPurple4', 'thistle1', 'thistle2', 'thistle3', 'thistle4']

# Set up the screen
screen = turtle.Screen()
screen.title("Atari Snake Game - Ahmad Sayad")
screen.bgcolor("black")
screen.setup(590, 625)
screen.tracer(0) # Turns off the screen updates

def mainMenu():
    print("We are in the main menu!")
    sideMenuGUI = turtle.Turtle()
    sideMenuGUI.color("white")
    sideMenuGUI.penup()
    sideMenuGUI.hideturtle()
    sideMenuGUI.goto(125,-300)
    sideMenuGUI.write("c3d4 @2018 Comp Sci. ", align="center", font=("DS-Terminal", 25, "normal"))


    mainMenuGUI = turtle.Turtle()
    mainMenuGUI.color("white")
    mainMenuGUI.penup()
    mainMenuGUI.hideturtle()
    mainMenuGUI.goto(0,100)
    mainMenuGUI.write("S N A K E", align="center", font=("DS-Terminal", 80, "normal"))
    
    # Animate
    for i in range(0,4,1):
        time.sleep(1)
        mainMenuGUI.clear()
        mainMenuGUI.goto(0, 100-50)
        mainMenuGUI.write("S N A K E", align="center", font=("DS-Terminal", 80, "normal"))
        time.sleep(1)
        mainMenuGUI.clear()
        mainMenuGUI.goto(0, 100)
        mainMenuGUI.write("S N A K E", align="center", font=("DS-Terminal", 80, "normal"))

    # GUI 
    controlsGUI = turtle.Turtle()
    controlsGUI.color("white")
    controlsGUI.penup()
    controlsGUI.hideturtle()
    controlsGUI.goto(0,-20)
    controlsGUI.write("-Press 'E' To Play-", align="center", font=("DS-Terminal", 40, "normal"))
    
    screen.listen()
    screen.onkeypress(Choose.chooseMenu, "E")
    
    screen.mainloop()

class Choose():
    TRACE = False
    RANDOM = False 
    def chooseMenu():
        print("We are in the options menu!")
   
        # Set up the screen
        screen.clear()
        screen.bgcolor("black")
        screen.setup(590, 625)
        screen.tracer(0) # Turns off the screen updates

        chooseTitleGUI = turtle.Turtle()
        chooseTitleGUI.color("white")
        chooseTitleGUI.penup()
        chooseTitleGUI.hideturtle()
        chooseTitleGUI.goto(0,230)
        chooseTitleGUI.write("Choose A Mode: ", align="center", font=("DS-Terminal", 40, "normal"))

        randomGUI = turtle.Turtle()
        randomGUI.color("white")
        randomGUI.penup()
        randomGUI.hideturtle()
        randomGUI.goto(0,50)
        randomGUI.write("Random [O/P]: {}".format(Choose.RANDOM), align="center", font=("DS-Terminal", 40, "normal"))

        tracerGUI = turtle.Turtle()
        tracerGUI.color("white")
        tracerGUI.penup()
        tracerGUI.hideturtle()
        tracerGUI.goto(0,-50)
        tracerGUI.write("Trace [T/F]: {}".format(Choose.TRACE), align="center", font=("DS-Terminal", 40, "normal"))

        continueGUI = turtle.Turtle()
        continueGUI.color("white")
        continueGUI.penup()
        continueGUI.hideturtle()
        continueGUI.goto(0,-250)
        continueGUI.write("-Press 'e' To Continu-", align="center", font=("DS-Terminal", 40, "normal"))

        def trace():
            Choose.TRACE = True
            tracerGUI.clear()
            tracerGUI.write("Trace [T/F]: True", align="center", font=("DS-Terminal", 40, "normal"))
            print("Trace: " + str(Choose.TRACE))
    
        def noTrace():
            Choose.TRACE = False
            tracerGUI.clear()
            tracerGUI.write("Trace [T/F]: False", align="center", font=("DS-Terminal", 40, "normal"))
            print("Trace: " + str(Choose.TRACE))
    
        def randomEvent():
            Choose.RANDOM = True
            randomGUI.clear()
            randomGUI.write("Random [O/P]: True", align="center", font=("DS-Terminal", 40, "normal"))
            print("Random: " + str(Choose.RANDOM))
    
        def noRandom():
            Choose.RANDOM = False
            randomGUI.clear()
            randomGUI.write("Random [O/P]: False", align="center", font=("DS-Terminal", 40, "normal"))
            print("Random: " + str(Choose.RANDOM))
    
        screen.listen()
        screen.onkeypress(trace, "T")
        screen.onkeypress(noTrace, "F")
        screen.onkeypress(randomEvent, "O")
        screen.onkeypress(noRandom, "P")
        screen.onkeypress(snakeGame, "e")

        screen.mainloop()

def snakeGame():

    print("You are playing snake!")
    # Create Special UI
    if Choose.RANDOM == True:
        delay = 0.05
    else:
        delay = 0.1
    
    print(Choose.TRACE)
    # Score
    score = 0

    # Set up the screen
    screen.clear()
    screen.bgcolor("black")
    screen.setup(590, 625)
    screen.tracer(0) # Turns off the screen updates

    if Choose.RANDOM == True:
        specialGUI = turtle.Turtle()
        for i in range(0, 2, 1):
            specialGUI.color("white")
            specialGUI.penup()
            specialGUI.hideturtle()
            specialGUI.goto(0, 0)
            specialGUI.write("ARE YOU READY TO RUMBLE", align="center", font=("DS-Terminal", 30, "normal"))
            time.sleep(.2)
            specialGUI.clear()
            specialGUI.color("red")
            specialGUI.write("ARE YOU READY TO RUMBLE", align="center", font=("DS-Terminal", 30, "normal"))
            time.sleep(.2)
            specialGUI.clear()
            specialGUI.color("white")
            specialGUI.write("ARE YOU READY TO RUMBLE", align="center", font=("DS-Terminal", 30, "normal"))
            time.sleep(.2)
            specialGUI.clear()
            specialGUI.color("red")
            specialGUI.write("ARE YOU READY TO RUMBLE", align="center", font=("DS-Terminal", 30, "normal"))
            time.sleep(.2)
            specialGUI.clear()
            specialGUI.color("white")
            specialGUI.write("ARE YOU READY TO RUMBLE", align="center", font=("DS-Terminal", 30, "normal"))
            time.sleep(.2)
            specialGUI.clear()
            specialGUI.color("red")
            specialGUI.write("ARE YOU READY TO RUMBLE", align="center", font=("DS-Terminal", 30, "normal"))
            time.sleep(.2)
            specialGUI.clear()
            specialGUI.color("white")
            specialGUI.write("ARE YOU READY TO RUMBLE", align="center", font=("DS-Terminal", 30, "normal"))
            time.sleep(.2)
        specialGUI.clear()

    def gameOver():
        gameOverGUI.write("Game Over!", align="center", font=("DS-Terminal", 50, "normal"))
        time.sleep(1)
        gameOverGUI.clear()
        gameOverGUI.color("red")
        gameOverGUI.write("Game Over!", align="center", font=("DS-Terminal", 50, "normal"))
        time.sleep(1)
        gameOverGUI.clear()
        gameOverGUI.color("white")
        gameOverGUI.write("Game Over!", align="center", font=("DS-Terminal", 50, "normal"))
        time.sleep(1)
        gameOverGUI.clear()
        gameOverGUI.color("red")
        gameOverGUI.write("Game Over!", align="center", font=("DS-Terminal", 50, "normal"))
        time.sleep(1)
        gameOverGUI.clear()
        snakeHead.goto(0,0)
        snakeHead.direction = "stop"
        Choose.chooseMenu()


    def dirUp():
        if snakeHead.direction != "down":
            snakeHead.direction = "up"

    def dirDown():
        if snakeHead.direction != "up":
            snakeHead.direction = "down"

    def dirLeft():
        if snakeHead.direction != "right":
            snakeHead.direction = "left"

    def dirRight():
        if snakeHead.direction != "left":
            snakeHead.direction = "right"

    def move():
        if snakeHead.direction == "up":
            y = snakeHead.ycor()
            snakeHead.sety(y + 20)

        if snakeHead.direction == "down":
            y = snakeHead.ycor()
            snakeHead.sety(y - 20)

        if snakeHead.direction == "left":
            x = snakeHead.xcor()
            snakeHead.setx(x - 20)

        if snakeHead.direction == "right":
            x = snakeHead.xcor()
            snakeHead.setx(x + 20)

    # Create play area
    playArea = turtle.Turtle()
    playArea.penup()
    playArea.goto(-280, 270)
    playArea.color("white")
    playArea.hideturtle()
    playArea.pendown()

    playArea.forward(555)
    playArea.right(90)
    playArea.forward(555)
    playArea.right(90)
    playArea.forward(555)
    playArea.right(90)
    playArea.forward(555)

    screen.listen()
    screen.onkeypress(dirUp, "Up")


    # Snake head
    snakeHead = turtle.Turtle()
    snakeHead.penup()
    snakeHead.speed(0)
    snakeHead.shape("square")
    snakeHead.color("white")
    snakeHead.goto(0,0)
    snakeHead.direction = "stop"
    if Choose.TRACE == True:
        snakeHead.pendown()
    else:
        snakeHead.penup()
    

    # Snake Pick Up
    pickUp = turtle.Turtle()
    pickUp.speed(0)
    pickUp.shape("square")
    pickUp.color("red")
    pickUp.penup()
    pickUp.goto(0,100)


    segments = []

    # GUI
    GUI = turtle.Turtle()
    GUI.color("white")
    GUI.penup()
    GUI.hideturtle()
    GUI.goto(-190, 270)
    GUI.write("Score: 0", align="center", font=("DS-Terminal", 35, "normal"))
    # Functions
    gameOverGUI = turtle.Turtle()
    gameOverGUI.color("white")
    gameOverGUI.penup()
    gameOverGUI.hideturtle()
    gameOverGUI.goto(0,0)

    # Keyboard bindings
    screen.listen()
    screen.onkeypress(dirUp, "Up")
    screen.onkeypress(dirDown, "Down")
    screen.onkeypress(dirLeft, "Left")
    screen.onkeypress(dirRight, "Right")

    # Main game loop
    while True:
        screen.update()

        # Game 
        # Check for a collision with the border
        if snakeHead.xcor()>260 or snakeHead.xcor()<-260 or snakeHead.ycor()>260 or snakeHead.ycor()<-260:
            gameOver()
            # Hide the segments
            for segment in segments:
                segment.goto(1000, 1000)
        
            # Clear the segments list
            segments.clear()

            # Reset the score
            score = 0

            # Reset the delay
            delay = 0.1

            GUI.clear()
            GUI.write("Score: {}".format(score), align="center", font=("DS-Terminal", 35, "normal"))


        # Check for a collision with the pickUp
        if snakeHead.distance(pickUp) < 20:
            # Add a segment
            nSegment = turtle.Turtle()
            nSegment.penup()
            nSegment.speed(0)
            nSegment.shape("square")
            nSegment.color("white")
            segments.append(nSegment)

            if (Choose.RANDOM == True):
                snakeHead.color(random.choice(colors))
                pickUp.color(random.choice(colors))
                nSegment.color(random.choice(colors))
                screen.bgcolor(random.choice(colors))
                GUI.color(random.choice(colors))
                screen.update()
            
            # Move the pickUp to a random spot
            pickUp.goto(random.randint(-260, 260), random.randint(-260, 260))

            # Shorten the delay
            delay -= 0.001

            # Increase the score
            score += 1
        
            GUI.clear()
            GUI.write("Score: {}".format(score), align="center", font=("DS-Terminal", 35, "normal"))

        # Move the end segments first in reverse order
        for index in range(len(segments)-1, 0, -1):
            x = segments[index-1].xcor()
            y = segments[index-1].ycor()
            segments[index].goto(x, y)

        # Move segment 0 to where the head is
        if len(segments) > 0:
            x = snakeHead.xcor()
            y = snakeHead.ycor()
            segments[0].goto(x,y)

        move()    

        # Check for head collision with the body segments
        for segment in segments:
            if segment.distance(snakeHead) < 20:
                gameOver()
        
                # Hide the segments
                for segment in segments:
                    segment.goto(1000, 1000)
        
                # Clear the segments list
                segments.clear()

                # Reset the score
                score = 0

                # Reset the delay
                delay = 0.1
        
                # Update the score display
                GUI.clear()
                GUI.write("Score: {}".format(score), align="center", font=("DS-Terminal", 35, "normal"))

        time.sleep(delay)
    screen.mainloop()

mainMenu()