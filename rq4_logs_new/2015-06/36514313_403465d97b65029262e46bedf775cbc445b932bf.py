# template for "Guess the number" mini-project
# input will come from buttons and an input field
# all output for the game will be printed in the console
import simplegui
import random

num_range=100
# helper function to start and restart the game
def new_game():
    # initialize global variables used in your code here
    global guesses
    global secret_number
    secret_number = random.randrange(0,num_range)
    print "New game. Range is from 0 to",num_range
    if num_range==100:
        guesses=7
    else:
        guesses=10
    print "Number of remaining guesses is",guesses
    
    print ""    
    # remove this when you add your code    



# define event handlers for control panel
def range100():
    # button that changes the range to [0,100) and starts a new game 
    global num_range
    num_range=100
    new_game()
    

def range1000():
    # button that changes the range to [0,1000) and starts a new game     
    global num_range
    num_range=1000
    new_game()

def input_guess(guess):
    # main game logic goes here	
    global guesses
    guess=int(guess)
    print "Guess was",guess
    guesses-=1
    if guess==secret_number:
        print "Correct"
        print ""
        new_game()

    else:
        if guesses==0:
            print "Number of remaining guesses is",guesses
            print "You ran out of guesses. The number was",secret_number
            print ""
            new_game()
        else:
            if guess>secret_number:
                print "Number of remaining guesses is",guesses
                print "Lower!"
            else :
                print "Number of remaining guesses is",guesses
                print "Higher!"
    print ""
    
# create frame
f=simplegui.create_frame("Guess the number",200,200)


# register event handlers for control elements and start frame
f.add_button("Range is [0,100)",range100,200)
f.add_button("Range is [0,1000)",range1000,200)
f.add_input("Enter a guess",input_guess,200)

# call new_game 
new_game()


# always remember to check your completed program against the grading rubric