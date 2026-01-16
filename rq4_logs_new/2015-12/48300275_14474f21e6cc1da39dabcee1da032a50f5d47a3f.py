
#-----Statement of Authorship----------------------------------------#
#
#  This is an individual assessment item.  By submitting this
#  code I agree that it represents my own work.  I am aware of
#  the University rule that a student must not act in a manner
#  which constitutes academic dishonesty as stated and explained
#  in QUT's Manual of Policies and Procedures, Section C/5.3
#  "Academic Integrity" and Section E/2.1 "Student Code of Conduct".
#
#    Student no: 9337407
#    Student name: Austin Wilshire
#
#  NB: Files submitted without a completed copy of this statement
#  will not be marked.  All files submitted will be subjected to
#  software plagiarism analysis using the MoSS system
#  (http://theory.stanford.edu/~aiken/moss/).
#
#--------------------------------------------------------------------#



#-----Task Description-----------------------------------------------#
#
#  BUBBLE CHARTS
#
#  This task tests your skills at defining functions, processing
#  data stored in lists and performing the arithmetic calculations
#  necessary to display a complex visual image.  The incomplete
#  Python script below is missing a crucial function,
#  "draw_bubble_chart".  You are required to complete this function
#  so that when the program is run it produces a bubble chart,
#  using data stored in a list to determine the positions and
#  sizes of the icons.  See the instruction sheet accompanying this
#  file for full details.
#
#--------------------------------------------------------------------#  



#-----Preamble and Test Data-----------------------------------------#
#
#

# Module provided
#
# You may use only the turtle graphics functions for this task;
# you may not import any other modules or files.

from turtle import *


# Given constants
#
# These constant values are used in the main program that sets up
# the drawing window; do not change any of these values.

max_value = 350 # maximum positive or negative value on the chart
margin = 25 # size of the margin around the chart
legend_width = 400 # space on either side of the window for the legend
window_height = (max_value + margin) * 2 # pixels
window_width = (max_value + margin) * 2 + legend_width # pixels
font_size = 12 # size of characters on the labels in the grid
grid_size = 50 # gradations for the x and y scales shown on the screen
tick_size = 5 # size of the ticks on either side of the axes, in pixels


# Test data
#
# These are the data sets that you will use to test your code.
# Each of the data sets is a list containing the specifications
# for several icons ("bubbles") to display on the screen.  Each
# such list element specifies one icon using four values:
#
#    [icon_style, x_value, y_value, z_value]
#
# The 'icon_style' is a character string specifying which icon to
# to display.  Possible icons are named 'Icon 0' to 'Icon 4'.
# The final three values are integers specifying the icon's values
# in three dimensions, x, y and z.  The x and y values will be in
# the range -350 to 350, and determine where to place the icon on
# the screen.  The z value is in the range 0 to 350, and determines
# how big the icon must be (i.e., its widest and/or highest size on
# the screen, in pixels).



# The first icon in three different sizes
data_set_00 = [['Icon 0', -200, 200, 20],
               ['Icon 0', 200, 200, 120],
               ['Icon 0', 0, 0, 100]]

# The second icon in four different sizes
data_set_01 = [['Icon 1', -200, 200, 20],
               ['Icon 1', 200, 200, 120],
               ['Icon 1', 200, -200, 60],
               ['Icon 1', 0, 0, 100]]

# The third icon in five different sizes
data_set_02 = [['Icon 2', -200, 200, 300],
               ['Icon 2', -200, -200, 30],
               ['Icon 2', 200, -200, 90],
               ['Icon 2', 200, 200, 120],
               ['Icon 2', 0, 0, 100]]

# The fourth icon in four different sizes
data_set_03 = [['Icon 3', -200, 200, 300],
               ['Icon 3', -200, -200, 30],
               ['Icon 3', 200, -200, 90],
               ['Icon 3', 0, 0, 100]]

# The fifth icon in four different sizes
data_set_04 = [['Icon 4', 200, 200, 190],
               ['Icon 4', -200, -200, 10],
               ['Icon 4', 200, -200, 90],
               ['Icon 4', 0, 0, 100]]

# The next group of data sets test all five of your icons
# at the same time

# All five icons at the same large size
data_set_05 = [['Icon 0', -200, 200, 200],
               ['Icon 1', 200, 200, 200],
               ['Icon 2', 200, -200, 200],
               ['Icon 3', -200, -200, 200],
               ['Icon 4', 0, 0, 200]]

# All five icons at another size, listed in a different order
data_set_06 = [['Icon 4', 0, 0, 150],
               ['Icon 3', -200, -200, 150],
               ['Icon 2', -200, 200, 150],
               ['Icon 1', 200, 200, 150],
               ['Icon 0', 200, -200, 150]]

# All five icons arranged diagonally, at increasing sizes
data_set_07 = [['Icon 0', -200, -200, 15],
               ['Icon 1', -100, -100, 50],
               ['Icon 2', 0, 0, 100],
               ['Icon 3', 100, 100, 120],
               ['Icon 4', 200, 200, 180]]

# An extreme test in which all five icons are VERY small
data_set_08 = [['Icon 0', -100, -80, 5],
               ['Icon 2', 100, -100, 1],
               ['Icon 3', 10, 30, 2],
               ['Icon 1', 100, 100, 0],
               ['Icon 4', 200, 200, 4]]

# The next group of data sets are intended as "realistic" ones
# in which all five icons appear once each at various sizes in
# different quadrants in the chart

# Data occurs in all four quadrants
data_set_09 = [['Icon 0', -265, -80, 50],
               ['Icon 2', 100, -146, 78],
               ['Icon 3', -50, 130, 69],
               ['Icon 1', 210, 100, 96],
               ['Icon 4', 200, 300, 45]]

# All data appears in the top quadrants
data_set_10 = [['Icon 4', -265, 80, 140],
               ['Icon 2', 100, 146, 24],
               ['Icon 1', 10, 30, 99],
               ['Icon 0', 210, 100, 75],
               ['Icon 3', 200, 300, 65]]

# All data appears in the top right quadrant
data_set_11 = [['Icon 3', 265, 80, 140],
               ['Icon 1', 100, 146, 24],
               ['Icon 2', 20, 30, 109],
               ['Icon 4', 210, 205, 75],
               ['Icon 0', 200, 300, 65]]

# All data appears in the bottom left quadrant
data_set_12 = [['Icon 2', -265, -110, 130],
               ['Icon 3', -100, -146, 34],
               ['Icon 0', -25, -40, 73],
               ['Icon 1', -210, -200, 75],
               ['Icon 4', -180, -320, 65]]

# Another case where data appears in all four quadrants
data_set_13 = [['Icon 4', -265, 80, 96],
               ['Icon 3', 100, -46, 54],
               ['Icon 2', 50, 30, 89],
               ['Icon 1', -210, -190, 75],
               ['Icon 0', 250, 300, 123]]

# Yet another set with data in all four quadrants
data_set_14 = [['Icon 1', 212, -165, 90],
               ['Icon 2', 153, -22, 125],
               ['Icon 3', 84, 208, 124],
               ['Icon 4', -105, -58, 85],
               ['Icon 0', -62, 274, 57]]

# A random test - it produces a different data set each time you run
# your program!
from random import randint
max_rand_coord = 300
min_size, max_size = 20, 200

data_set_15 = [[icon,
                randint(-max_rand_coord, max_rand_coord),
                randint(-max_rand_coord, max_rand_coord),
                randint(min_size, max_size)]
               for icon in ['Icon 0', 'Icon 1', 'Icon 2', 'Icon 3', 'Icon 4']]

# Finally, just for fun, a random test that produces a montage
# by plotting each icon twenty times (which obviously doesn't
# make sense as a real data set)
data_set_16 = [[icon,
                randint(-max_rand_coord, max_rand_coord),
                randint(-max_rand_coord, max_rand_coord),
                randint(min_size, max_size)]
               for icon in ['Icon 0', 'Icon 1', 'Icon 2', 'Icon 3', 'Icon 4'] * 20]

#***** If you want to create your own test data sets put them here

#
#--------------------------------------------------------------------#

###The specific coords for the legend when it is drawn###
legend = [['Icon 0', 370, 250, 75],
               ['Icon 1', 370, 150, 75],
               ['Icon 2', 370, 50, 75],
               ['Icon 3', 370, -50, 75],
               ['Icon 4', 370, -150, 75]]


#-----Student's Solution---------------------------------------------#
#
#  Complete the task by replacing the dummy function below with
#  your code

#Abritrary value is assigned here, and is changed
#in the draw_bubble_chart() function to the proper value
height = 0
#These are used to access the appropriate sections in the list
#according to the value they change
x = 1
y = 2
z = 3


def star(height, colour):

    left_angle = 72 # left turn angle in degrees
    right_angle = 144  # right turn angle in degrees
    line_length = height * 0.409 # length of each of the ten lines

    # Drawing 5 star points
    setheading(-left_angle) # pointing down from top point
    color(colour) # the colour in the functions parameters
    pendown()
    begin_fill()
    for i in range(5): # draw each of the segments
      forward(line_length)
      left(left_angle)
      forward(line_length)
      right(right_angle)
    end_fill()
    penup()

#Used to draw different coloured and sized, filled in rectangles
def rect(colour, width, height):
    pendown()
    color(colour)#Uses colour provided in the parameters
    begin_fill()#Starts the fill
    forward(width)
    left(90)#Uses left() instead of setheading() to be more flexible
    forward(height)
    left(90)
    forward(width)
    left(90)
    forward(height)
    end_fill()
    penup()

##Draws a rectangle with the outline only###
def clear_rect(colour, width, height):
    pendown()
    
    color(colour)# Determines the outline's colour
    forward(width)
    left(90)
    forward(height)
    left(90)
    forward(width)
    left(90)
    forward(height)
    
    penup()
    
#Draws an equilateral triangle
def triangle(colour, length):
    # draw an equilateral triangle with tess
    begin_fill()
    color(colour)#Uses colour prvided in the parameters of the function
    setheading(180)
    forward(length)#Length provided in the parameters
    left(120)
    forward(length)
    left(120)
    forward(length)
    left(120)
    end_fill()

        
def captain_america(height):
    pendown()
    setheading(270)

    ###Draws the first circle###
    begin_fill()
    color('red')
    circle(height)#Using hieght keeps integrity of shapes#
    end_fill()
    penup()

    ##Goes to the appropriate position for the next circle##
    goto(xcor()+(height/5.7), ycor())
    ###Follows the order of 'go to a place, start the fill, draw a circle, end the fill go to next place'##
    

    begin_fill()
    color('white')
    circle(height/1.2)
    end_fill()

    goto(xcor()+(height/6), ycor())

    begin_fill()
    color('red')
    circle(height/1.5)
    end_fill()

    goto(xcor()+(height/6), ycor())
    
    begin_fill()
    color('blue')
    circle(height/2)
    end_fill()

    ##Goes to position for the star##
    goto(xcor()+(height/2.05), ycor()+(height/4))

    ##Draws a star using the star() function##
    star(height/2,'white')
    
    penup()
    

def thor(height):
    pendown()

    ###Firct Circle###
    begin_fill()
    color('#878787')
    circle(height)
    end_fill()

    ##Next position of the next circle to be drawn##
    goto(xcor()+(height/5.7), ycor())

    ###Second Circle###
    begin_fill()
    color('black')
    circle(height/1.2)
    end_fill()

    goto(xcor()+(height/3), ycor())
    setheading(0)

    ###Hammer Head###
    rect('#878787', height, height/2)
    goto(xcor() + height/3.9, ycor()+height/2)
    setheading(0)
    rect('#878787', height/2, height/8)

    penup()
    goto(xcor() + height/11, ycor() - height/1.5)
    
    ###Handle for Hammer###
    setheading(0)
    pendown()
    #First rectangle#
    rect('#945400', height/3, height/8)

    ##Draws all subsequent rectangles with efficiently##
    for i in range(1,4):
        penup()
        setheading(270)
        forward(height/5)
        setheading(0)
        pendown()
        rect('#945400', height/3, height/8)
    
    penup()

def hulk(height):
    pendown()

    ###Firct Circle###
    begin_fill()
    color('purple')
    circle(height/2)
    end_fill()

    goto(xcor()+(height/11.4), ycor())

    ###Second Circle###
    begin_fill()
    color('black')
    circle(height/2.4)
    end_fill()

    goto(xcor()+(height/2.8), ycor())

    ###Middle circle###
    begin_fill()
    color('green')
    circle(height/16)
    end_fill()
    penup()

    ##Position of first rectangle##
    setheading(120)
    forward(height/8)
    pendown()

    ###Out side Rectangles###

    ##Set the heading so the rectangle is drawn on a tilt##
    setheading(140)
    rect('green', height/5.6, height/8)
    setheading(0)
    penup()
    ##Move to next position##
    goto(xcor(), ycor() - height/50)
    ##Next rectangle##
    goto(xcor() + height/3.2, ycor() - height/20)
    ##Next position##
    setheading(40)
    rect('green', height/5.6, height/8)
    penup()
    ##Next position##
    goto(xcor() - height/8, ycor() - height/2.7)
    setheading(90)
    rect('green', height/5.6, height/8)

    penup()

def iron_man(height):
    pendown()

    
    #Firct Circle
    begin_fill()
    color('red')
    circle(height/2)
    end_fill()
    penup()
    goto(xcor()+(height/11.4), ycor())
    #Second Circle
    begin_fill()
    color('yellow')
    circle(height/2.4)
    end_fill()

    goto(xcor()+(height/1.2), ycor()+height/4)

    #The triangles
    triangle('red',height/1.2)
    goto(xcor()-height/16, ycor()-height/30)
    triangle('yellow', height/1.4)

    goto(xcor()-height/9, ycor()-height/15)
    triangle('red', height/2)
    goto(xcor()-height/12, ycor()-height/17)
    triangle('yellow', height/3)

    #This section of is where the abitrary lines around the picture are drawn
    #It follows a simple formula of 'go here, draw that length, go to next'

    ###Lines Leading to the triangles###
    goto(xcor()+height/20, ycor()+height/30)
    setheading(35)
    pendown()
    width(height/30)
    color('red')
    forward(height/5)
    penup()
    
    goto(xcor()-height/1.3, ycor())
    pendown()
    setheading(325)
    pendown()
    width(height/30)
    color('red')
    forward(height/5)
    penup()

    goto(xcor()+height/4.4, ycor()-height/3)
    pendown()
    setheading(270)
    pendown()
    width(height/30)
    color('red')
    forward(height/5)
    penup()

    ###Lines outside the triangles connecting to circles###

    ##Top two lines##
    goto(xcor()-height/6, ycor()+height/1.2)
    pendown()
    setheading(285)
    pendown()
    width(height/25)
    color('red')
    forward(height/5.5)
    penup()

    goto(xcor()+height/4, ycor())
    pendown()
    setheading(75)
    pendown()
    width(height/25)
    color('red')
    forward(height/5.5)
    penup()
    
    ##Left lines##
    
    goto(xcor()-height/1.7, ycor()-height/1.7)
    pendown()
    setheading(25)
    pendown()
    width(height/25)
    color('red')
    forward(height/5)
    penup()

    goto(xcor()+height/10, ycor()-height/7)
    pendown()
    setheading(230)
    pendown()
    width(height/25)
    color('red')
    forward(height/5.5)
    penup()

    ##Right Lines##

    goto(xcor()+height/1.9, ycor())
    pendown()
    setheading(140)
    pendown()
    width(height/25)
    color('red')
    forward(height/5.5)
    penup()

    goto(xcor()+height/12, ycor()+height/7)
    pendown()
    setheading(340)
    pendown()
    width(height/25)
    color('red')
    forward(height/5.5)
    penup()

    width(1)

def SHIELD(height):
    pendown()
    width(1)
    
    #Firct Circle
    begin_fill()
    color('black')
    circle(height/2)
    end_fill()

    goto(xcor()+(height/11.4), ycor())

    #Second Circle
    begin_fill()
    color('white')
    circle(height/2.4)
    end_fill()
    
    penup()
    goto(xcor() + height/2.5, ycor())
    
    #The Head
    begin_fill()
    color('black')
    setheading(25)
    pendown()
    forward(height/3)
    setheading(110)
    forward(height/9)
    setheading(210)
    forward(height/5)
    ########
    
    #The neck
    setheading(105)
    forward(height/7)
    setheading(0)
    forward(height/10)
    setheading(105)
    forward(height/14)
    setheading(180)
    forward(height/8)
    setheading(250)
    forward(height/5)

    ########
    setheading(150)
    forward(height/5)
    setheading(240)
    forward(height/9)
    setheading(330)
    forward(height/3)
    
    end_fill()

    ####################

    #The Tail

    penup()
    setheading(90)
    forward(-height/50)
    
    pendown()
    begin_fill()
    setheading(240)
    forward(height/3.5)
    setheading(270)
    circle(height/6, 180)
    setheading(125)
    forward(height/3.5)
    end_fill()
    penup()

    ##The Wings##

    #Left Side
    
    goto(xcor() - height/3.2, ycor() + height/6)
    setheading(240)
    rect('black', height/6, height/12)

    setheading(330)
    forward(height/9)
    setheading(240)
    rect('black', height/4.5, height/12)

    setheading(330)
    forward(height/9)
    setheading(240)
    rect('black', height/3, height/12)

    #transition to right side
    goto(xcor() + height/2, ycor())

    #Right Wing
    
    goto(xcor(), ycor() - height/20)
    setheading(120)
    rect('black', height/6, height/12)


    setheading(235)
    forward(height/8.8)
    setheading(120)
    rect('black', height/4.5, height/12)

    
    setheading(235)
    forward(height/17)
    setheading(90)
    forward(-height/10)
    setheading(120)
    rect('black', height/3, height/12)
    
    

def draw_bubble_chart(data_set):
        #Get these variables that were declared before the star() function
        #Make them accessible and chnageable within this function
        global x, y, z
        
        #Iterates through the given data set's length,
        #and uses this to determine what set is being used
        for i in range(0,len(data_set)):
            #an easy variable to access the lists in the if-else block
            #not a variable named something confusing and abstract
            data = data_set[i] 
            ##Checks the 0 position in the list for 'Icon 0'
            if data[0] == 'Icon 0':
                width = data[3]/2 #sets the height for a circle
                
                #Goes to the positon where center
                #is at the provided coordinate
                goto(data[x]-width,data[y])
                
                #draw captain america by calling the function
                captain_america(width)#uses the width variable as the parameter for the function
                
                #turns the trutle the correct way, needed to make sure
                #the captain_america() function works properly
                setheading(270)
            elif data[0] == 'Icon 1':
                
                width = data[3]/2 #Because it is a circle
                
                goto(data[x]-width,data[y])
                thor(width)
                #turns the trutle the correct way, needed to make sure
                #the thor() function works properly
                setheading(270)
                
            elif data[0] == 'Icon 2':
                
                width = data[3]
                goto(data[x]-width/2,data[y])
                hulk(width)
                #turns the trutle the correct way, needed to make sure
                #the hulk() function works properly
                setheading(270)

            elif data[0] == 'Icon 3':
                
                width = data[3] 
                goto(data[x]-width/2,data[y])
                iron_man(width)
                #turns the trutle the correct way, needed to make sure
                #the iron_man() function works properly
                setheading(270)
                
            elif data[0] == 'Icon 4':
                
                width = data[3] 
                goto(data[x]-width/2,data[y])
                SHIELD(width)
                #turns the trutle the correct way, needed to make sure
                #the SHIELD() function works properly
                setheading(270)
                
            print data
#Draws the legend#           
def draw_legend(data_set):
    goto(365,350)
    setheading(270)
    width(3)
    clear_rect('black',600, 200)
    width(1)
    goto(400, 300)
    #Title for the legend#
    write('The Avengers!', font=('Arial', 15, 'italic'))
    #Get these variables that were declared before the star() function
        #Make them accessible and chnageable within this function
    global height, x, y, z    
    print height
    #icon 1
    for i in range(0,len(data_set)):
        data = data_set[i]
        if data[0] == 'Icon 0':
            height = data[3]/2
            
            goto(data[x],data[y])
            
            captain_america(height)
            goto(xcor()+height*1.2, ycor()-height/2.5)#Go to the position were the text will be written
            color('black')#Changes the colour to black for the text
            write('Captain America', font=('Arial', 10, 'normal'))#Writes the text
            #turns the trutle the correct way, needed to make sure
            #the captain_america() function works properly
            setheading(270)
        elif data[0] == 'Icon 1':
            
            height = data[3]/2 #Because it is a circle
            
            goto(data[x],data[y])
            thor(height)
            goto(xcor()+height*2.2, ycor()+height/2)
            color('black')
            write('Thor', font=('Arial', 10, 'normal'))
            #turns the trutle the correct way, needed to make sure
            #the thor() function works properly
            setheading(270)
            
        elif data[0] == 'Icon 2':
            
            height = data[3]
           
            goto(data[x],data[y])
            
            hulk(height)
            goto(xcor()+height/1.3, ycor()+height/4)
            color('black')
            write('The Hulk', font=('Arial', 10, 'normal'))
            #turns the trutle the correct way, needed to make sure
            #the hulk() function works properly
            setheading(270)

        elif data[0] == 'Icon 3':
            
            height = data[3] 
            
            goto(data[x],data[y])
            iron_man(height)
            goto(xcor()+height/2, ycor()+height/6)
            color('black')
            write('Iron Man', font=('Arial', 10, 'normal'))
            #turns the trutle the correct way, needed to make sure
            #the iron_man() function works properly
            setheading(270)
            
        elif data[0] == 'Icon 4':
            
            height = data[3] 
            
            goto(data[x],data[y])
            SHIELD(height)
            goto(xcor()+height/2, ycor()+height/4)
            color('black')
            write('S.H.I.E.L.D', font=('Arial', 10, 'normal'))
            #turns the trutle the correct way, needed to make sure
            #the SHIELD() function works properly
            setheading(270)
            
        print data

#
#--------------------------------------------------------------------#



#-----Main Program---------------------------------------------------#
#
# This main program sets up the drawing environment, ready for you
# to start drawing your bubble chart.  Do not change any of
# this code except the lines marked '*****'
    
# Set up the drawing window with enough space for the grid and
# legend
setup(window_width, window_height)
title('The Avengers!') #***** Choose a title appropriate to your icons

# Draw as quickly as possible by minimising animation
#hideturtle()     #***** You may comment out this line while debugging
                 #***** your code, so that you can see the turtle move
speed('faster') #***** You may want to slow the drawing speed
                 #***** while debugging your code

# Choose a neutral background colour                    
bgcolor('grey')

# Draw the two axes
pendown() # assume we're at home, facing east
forward(max_value)
left(180) # face west
forward(max_value * 2)
home()
setheading(90) # face north
forward(max_value)
left(180) # face south
forward(max_value * 2)
penup()

# Draw each of the tick marks and labels on the x axis
for x_coord in range(-max_value, max_value + 1, grid_size):
    if x_coord != 0: # don't label zero
        goto(x_coord, -tick_size)
        pendown()
        goto(x_coord, tick_size)
        penup()
        write(str(x_coord), align = 'center',
              font=('Arial', font_size, 'normal'))
        
# Draw each of the tick marks and labels on the y axis
for y_coord in range(-max_value, max_value + 1, grid_size):
    if y_coord != 0: # don't label zero
        goto(-tick_size, y_coord)
        pendown()
        goto(tick_size, y_coord)
        penup()
        goto(tick_size, y_coord - font_size / 2) # Allow for character height
        write('  ' + str(y_coord), align = 'left',
              font=('Arial', font_size, 'normal'))

# Call the student's function to display the data set
draw_bubble_chart(data_set_05) #***** Change this for different data sets
draw_legend(legend)#Uses a custom data set to draw the legend   
# Exit gracefully
hideturtle()
done()

#
#--------------------------------------------------------------------#



