from sys import exit 

score = 0
up = "up"
down = "down"

def gold_room():
    print "This room is full of gold.  How much do you take?"
    
    choice = raw_input("> ") #choice in this and subsequent are local vars
    how_much = int(choice)
    # if "0" in choice or "1" in choice: old way, was stupid
    # Also, int can turn strings into integers...boo
    # I thought abt converting string values to hashes for a no-error solution
    # but then I remembered I'm an asshole, and I searched far and wide through
    # the builtins until I found the isdigit string method. I would've really
    # had no idea how to write a typing function if one didn't exist.
    if choice.isdigit() == False:
        score_adjust(down, 100, score)
        dead("Man, learn to type a number.") 
    elif how_much < 50: #hey, it was a number! passed down here from first if
        score_adjust(up, (how_much**3), score)
        print "Nice, you're not greedy, you win!"
        print "Your score: %d" % score
        exit(0)
    else: 
        score_adjust(down, (how_much**2), score)
        dead("You greedy bastard! I hope it was worth it.")


def bear_room(): # takes no arguments
    print "There is a bear here." # for user's benefit
    print "The bear has a bunch of honey."
    print "The fat bear is in front of another door."
    print "How are you going to move the bear?"
    
    bear_moved = False

    while True: 
    #of note that this func uses while, whereas Cthulhu recurs manually
        
        choice = raw_input("> ")
        global score 
        # Really gonna feel like an asshole if I have to global in every loop 
        if choice == "take honey":
            score_adjust(down, 20, score)
            dead("The bear looks at you then slaps your face off.") #womp womp
        elif choice == "taunt bear" and not bear_moved: #True at outset
            score_adjust(up, 10, score)
            print "The bear has moved from the door. You can go through it now."
            bear_moved = True 
            # fuck scope. The mouthwash and the principle of programming
        elif choice == "taunt bear" and bear_moved:
            score_adjust(down, 20, score)
            dead("The bear gets pissed off and chews your leg off.")
        elif choice == "open door" and bear_moved:
            score_adjust(up, 50, score)
            gold_room() 
        else:
            print "I got no idea what that means." 

def cthulhu_room(): # anyway
    print "Here you see the great evil Cthulhu."
    print "He, it, whatever stares at you and you go insane."
    print "Do you flee for your life or eat your head?"

    choice = raw_input("> ")

    if "flee" in choice: # obvious
        score_adjust(up, 15, score)
        start()
    elif "head" in choice:
        score_adjust(up, 5, score)
        dead("Well that was tasty!")
    else: 
        print "You WILL answer to Cthulhu."
        cthulhu_room()

def extra_room():
    print "Hey look, another room."
    print "Your thoughts?"
    
    global score
    choice = raw_input("> ")
    
    if "look" in choice:
        print "Not much in here besides a bottomless pit. No one likes those."
        extra_room()
    elif "stupid" in choice or "dumb" in choice:
        score_adjust(up, 5, score)
        print "Agreed. Let's go back."
        start()
    else:
        score_adjust(down, 25, score)
        print "Lost in your reverie, you fail to notice the bottomless pit in here."
        dead("Look where you're going next time.")

def dead(why): 
    print why, "Good job!"
    print "Your score: %d" % score # cant' have a text adventure w/o scoring
    exit(0)

def score_adjust(updown, points, scr): 
# Scoring function finally working. I don't even know what I learned,
# Other than I don't need return if I am already updating a value,
# and also that global variables have to be explicitly declared.
# I added the print lines so I could see the score update after each move
# as a way to troubleshoot.
    if updown == "up":
        global score
        score = scr + points
        # print score
    elif updown == "down":
        global score
        score = scr - points
        # print score
    

def start(): # handles initial prompt and sends you to other rooms
    print "You are in a dark room."
    print "There is a door to your right and left, and one straight ahead."
    print "Which one do you take?"

    choice = raw_input("> ") # choice is always local to funcs

    if choice == "left":
        score_adjust(up, 10, score)
        bear_room()
    elif choice == "right":
        score_adjust(up, 10, score)
        cthulhu_room()
    elif choice == "straight":
        score_adjust(up, 10, score)
        extra_room()
    else:
        score_adjust(down, 5, score)
        print "Holy crap dude."
        print "Try doing it right this time."
        print "Type (left, right, straight)"
        print "Now then, where were we?"
        start()


start() # enter the flowchart