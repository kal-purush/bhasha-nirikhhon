# HangManGame.py
# A simple single-player text-based implementation of the Hang Man game.
#
# Author: Siu Lok Man Lian
#
# No license
"""
Documentation:

In this implementation, player needs to enter an alphabets or a word with
alphabets in every guess input. Besides, all text inputs required are
numbers or 'yes' and 'no'. 

After reading the game rules, player can start a new game by choosing a 
game level. 3 levels are available, Beginner, Medium, and Expert. There
are approximately a total of 360 words provided, with 120 words for each 
level. The program will randomly choose a word from the 120 words after 
player had chose a game level.

With the randomly chosen word, the game begins with asking player to enter
his or her first guess. All guesses can be either an alphabet or a word 
which must be alphabets only. Other inputs including empty inputs, spaces, 
numbers, or other symbols will be rejected.

If the input for guess is valid, the program will perform algorithms to check
that guess is correct or wrong. A result message will be shown after each 
guess no matter it is correct or wrong. 

Player can make guesses continuously until 7 wrong guesses are made, i.e. the
whole hang man is drawn and displayed. The hang man will be displayed only 
when wrong guesses are made. Player needs to close the hang man window before
entering next guess. 

During the game, player can choose to quit anttime by enterintg the word "quit"
as guess. If player choose to quit, the game will not be saved and player needs
to start a new game if he or she chose to quit. Confirmation is needed to quit
the game program. If player do not want to quit, he or she can choose to go 
back to the game menu to start another game.

Player inputs are validate by the 2 input validation functions, which are 
designed for the two main types of inputs in this implementation, numbers and
alphabets. Error message will be displayed when user inputs are in valid.

There are 9 coordinate system functions, including functions for handling 
the quit game function, some confirmation functions and some functions before 
starting the game, like demo display, rule display, choosing game level, etc.

With these functions, there may be more potential bugs but it also generates
more possibilities for the game, which can personalize this common game. 

For the 8 game logic functions, they are more complicated and contains more 
calculations as there are too many possibilities to predict and too many 
assumptions need to made to prevent from bugs. Therefore, many variables with
True/False values are set to avoid errors in the complicated calculations.

The 4 display functions are mainly done by graphics and the built-in functions 
of strings and lists. There are almost no GUI components as the hang man game
need not to be that fancy and complicated according to my childhood memory. 
Therefore, the style of this game is very simple as you can see, just a
graphic window and others are all text and text.

Here are some statistics for reference, there are total of 13 for-loops, 64 if
or elif-statements, more than 10 variables, 23 working functions, few list, 
strings, boolean operator. Besides os, I have imported 3 non nuilt-in modules,
including random (for choosing words), graphics (for hang man display), and
time (for program sleep). Also, one of the math operation is used for getting
and calculating mouse click position in the hang man display function. Files
(for rules display and choose word from the 3 word libraries) are read in rules
function and chooseWord function.

There are only three lines of codes in the test() as the program is designed 
with deep linkings between functions. Every functions links each other and bugs
are already removed and hopefully its number is already minimized.

I believed the explanations commented are clearly stated and understandable.
Hope you enjoy the game!
"""
import os
import random
import graphics
from graphics import *
import time

os.chdir("/Users/LianSiu/Downloads/COMP1001_Project_14040502D")

## Constants ##

# set up alphabetsList storing the 26 alphabets for checking if the input is an alphabet
alphabetsList=["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"]


## Input validation functions ##

def checkZeroOne(response):
    """Validate the 1 and 0 inputs"""

    # return True if input is one or zero
    if(response=="1")or(response=="0"):
        return True
    
    # return False and print error message if input is not one or zero
    else:
        print("Your input can only be 1 or 0, please enter again!")
        return False


def checkInput(newGuess):
    """Check if user input is an alphabet or alphabets"""

    # check every alphabet(s) in the newGuess word
    for letter in newGuess:
        char=0
        
        # compare letter with every alphabets in alphabetsList
        for alphabet in alphabetsList:
            if alphabet==letter :
                char +=1

        # return False if letter is not found in the alphabetsList
        if char==0:
            return False


## Coordinate system functions ##

def rules():
    """Display game rules and call function for game demonstration"""

    # Formatting the game rules diaplay
    print("Game Rules".center(75,"-"),"\n")

    # open the text file storing the game rules,then read and print out the text using the file handler
    rules = open("rules.txt","r")
    for line in rules.readlines():
        print(line,end="")
    print("\n")
    print("-"*75)
    print("\n")

    # program sleep for 10 seconds to let user to read the game rules
    time.sleep(10)


def Demo():
    """Provide game demonstration (showing how the hang man looks like)"""

    print("A demo (how the HangMan looks like when you lose the game) is provided.")

    # ask if user want to see the demo
    demo=input("Enter 1 to open the demo graph in a new window (recommended!) or enter 0 to \nskip this demo:\n")

    # validate user input
    if(checkZeroOne(demo)==False):
        Demo()

    else:
        # run this part if user choose to see demo
        if(demo =="1"):
            #set variable demo as true
            demo=True
            wrongGuess=0
            count="nil"
            chooseWord="nil"

            #call function to display HangMan demo
            hangMan(demo,wrongGuess,count,chooseWord)

        # directly run this part if user don't want to see demo, a reminder about how to quit the game
        print("\n"+"#"*75)
        print("\n*** Please be noted that you can quit this game anytime by entering \n    the word \"quit\" as your guesses in the game.\n")
        print("#"*75+"\n")

        # program sleep for 5 seconds to let user to read the reminder
        time.sleep(5) 

        # jump to next part for starting a new game
        rulesCheck()


def rulesCheck():
    """Ask if user is ready to start a new game"""

    # ask if user is ready for a new game
    rulescheck = (input("If you are ready, enter 1 to start a new game or enter 0 to quit this game:\n"))

    # validate user input
    if(checkZeroOne(rulescheck)==False):
        rulesCheck()

    # run this part if input is valid
    else:
        # go to next part if user is ready
        if rulescheck =="1":
            Level()
        # run this part if user choose to quit
        elif rulescheck =="0":
            confirmQuit()

        
def Level():
    """Start a new game by asking user to choose a game level"""

    print("\nPlease choose the game level to start a new game.")

    # ask user to input a number (1 or 2 or 3) for level selection, or input zero to quit
    level=input("\nEnter 1 for Beginner, 2 for Medium, 3 for Expert,or enter 0 to quit this game: \n")

    # run this part if user choose to quit
    if level=="0":
        confirmQuit()

    # jump to next part for confrming user choice on selected level if input is valid level number
    elif ((level=="1")or(level=="2")or(level=="3")):
        confirmChoice(level)

    # print a error message if input is invalid and ask for input again
    else:
        print("Your input can only be 0, 1, 2 or 3, please enter again!")
        Level()



def confirmChoice(level):
    """Ask user to confirm his or her choice on game level"""

    # assign appropriate string to variable gameLevel according to the level selected
    if level =="1":
        gameLevel = "Beginner"
        
    elif level=="2":
        gameLevel = "Medium"
        
    elif level=="3":
        gameLevel = "Expert"

    # display the selected level to user and ask for confirmation
    print("\nYour choice is "+level+", the "+gameLevel+" level.")
    confirm = input("Enter 1 to confirm your choice, or enter 0 to go back:\n")

    # validate user input
    if(checkZeroOne(confirm)==False):
        confirmChoice(level)

    # run this part if input is valid
    else:
        # jump to getWord() for getting the file handler of the word list at the selected level from text file
        if confirm =="1":
            getWord(level)
            
        # go back to level selection if user want to choose again
        elif confirm =="0":
            Level()


def getWord(level):
    """Get the word list at the level chose by user"""

    # get the file handler of the corresponding text file
    if level=="1":
        word = open("WordLibBeginner.txt","r")
        
    elif level=="2":
        word = open("WordLibMedium.txt","r")
        
    elif level=="3":
        word = open("WordLibExpert.txt","r")

    # call chooseWord() to select a word from the word list randomly
    chooseWord(word)


def chooseWord(word):
    """Randomly choose a word from the word list"""

    # read all the words from the text file by file handler and store them in a list named wordList
    wordList =[]
    for line in word.readlines():
        wordList+= line.split(",")

    # randomly choose a word from wordList
    chooseWord = random.choice(wordList)

    # start a new game with the selected word named chooseWord
    start(chooseWord)
    

def confirmQuit():
    """Called when user want to quit the game"""

    # ask if user really want to quit the game
    confirmquit=input("\nAre you sure you want to quit? \nEnter yes to quit or enter no to go back to game menu: ")
    global exit

    # if user choose yes, print the goodbye message
    if confirmquit=="yes":
        print("\n")
        print("Thank you for playing!")
        exit=True
        #exit()

    # back to game menu if user do not want to quit
    elif confirmquit=="no":
        rulesCheck()

    # print error statement if user input is invalid and ask again
    else:
        print("Your input can only be yes or no, please enter again!")
        confirmQuit()



def forceQuit(chooseWord,count,guessList,wrongGuess,currentStr,guessDisplay):
    """Force to quit during the game"""

    # ask if user really want to quit during the game as game cannot be saved
    quit=input("You cannot get back to this game if you choose to quit!\nEnter 1 to leave this game or enter 0 to resume:")

    # validate user input
    if(checkZeroOne(quit)==False):
        forceQuit(chooseWord,count,guessList,wrongGuess,currentStr,guessDisplay)

    # run this part if input is valid
    else:
        # run this part if user choose to quit the game
        if(quit=="1"):
            confirmQuit()

        # run this part to resume
        else:
            game(chooseWord,count,guessList,wrongGuess,currentStr,guessDisplay)


## Game logic functions ##

def start(chooseWord):
    """Start a new game with a word chosen randomly by computer"""

    # display the length of the randomloy selected word to the user
    wordLength = str(len(chooseWord))
    print("The word contains "+wordLength+" alphabets.")

    # set up variables for the game
    count=0
    guessList=[]
    wrongGuess=0
    currentStr=[]
    guessDisplay=[]

    # put spaces into current string for display after each guess
    for b in range(len(chooseWord)):
        currentStr+=" "

    # start the game
    game(chooseWord,count,guessList,wrongGuess,currentStr,guessDisplay)


def game(chooseWord,count,guessList,wrongGuess,currentStr,guessDisplay):
    """Asking user for new guesses"""

    # ask user to input his or her guess in a word or a letter
    newGuess= input("Enter your guess (an alphabet or a word, in lowercase): ")

    # run this part if the input is or are not alphabet(s)
    if (checkInput(newGuess)==False):
        # error message for input with spaces
        if (newGuess==" "):
            print("You cannot input spaces!")

        # error message for other non-alphabets inputs
        else:
            print("Input should be alphabet(s) in lowercase only!")

        # ask for new guess again
        game(chooseWord,count,guessList,wrongGuess,currentStr,guessDisplay)

    # run this part if the input is empty, display error message and ask for new input
    elif (newGuess==""):
        print("Empty input is not allowed!")
        game(chooseWord,count,guessList,wrongGuess,currentStr,guessDisplay)

    # run this part if user input "quit" and call function to ask if user really want to quit
    elif (newGuess=="quit"):
        forceQuit(chooseWord,count,guessList,wrongGuess,currentStr,guessDisplay)

    # run this part if input is valid
    else:
        print("Your guess is "+newGuess+".")

        # ask user to confirm his or her guess
        confirmGuess = input("Enter 1 to confirm or enter 0 to enter again:\n")

        # run this part if user confirmed the guess
        if confirmGuess =="1":
            # run this part if it is the first guess
            if count==0:
                repeatchecked=False
                # call checkGuess() to determine the guess is correct or not
                checkGuess(newGuess,guessList,chooseWord,wrongGuess,count,currentStr,guessDisplay,repeatchecked)

            #run this part if it is not the first guess, call checkRepeatGuess to check if the guess is repeated before calling checkGuess()
            else:
                checkRepeatGuess(currentStr,newGuess,guessList,chooseWord,count,wrongGuess,guessDisplay)

        # go back and ask for a new user input if user want to make another guess
        elif confirmGuess=="0":
            game(chooseWord,count,guessList,wrongGuess,currentStr,guessDisplay)

        # print error message and go back to previous stage if input for guess confirmation is invalid
        else:
            print("Your input can only be 0 or 1, please input your guess again!")
            game(chooseWord,count,guessList,wrongGuess,currentStr,guessDisplay)


def checkGuess(newGuess,guessList,chooseWord,wrongGuess,count,currentStr,guessDisplay,repeatchecked):
    """Validate the guess entered by user every times, check if it matches the chosen word"""

    # insert the new guess into the guess list for display according to their positions
    # insert as the first item if it is the first guess
    if repeatchecked==False:
        guessList.insert(0,newGuess)

    # insert as the last item if it is not the first guess
    elif repeatchecked==True:
        last=len(guessList)
        guessList.insert(last,newGuess)

    # run this part if the guess is an alphabet    
    if len(newGuess)==1:
        # set a new variable wrong to count the wrong result in the comparison with the chooseWord (the answer)
        wrong=0

        # check with every letter in the chooseWord
        for y in range (len(chooseWord)):
            compareAlphabets=chooseWord[y]
            # set variables last and check as indicators on the status of the newGuess
            last=False
            check="wrong"
            # update the value of check and update the string if the newGuess matches any alphabet(s) in chooseWord
            if newGuess == compareAlphabets:
                check="correct"
                updateString(guessList,wrongGuess,newGuess,chooseWord,count,check,last,currentStr,guessDisplay,y)

            # add one to wrong whenever the newGuess does not match any alphabets in the chooseWord
            else:
                wrong+=1

        # run this part if the checking of chooseWord and newGuess reached the end        
        if (compareAlphabets==(chooseWord[-1])):
            # update the value of last as True to indicate it is the last checking for the current newGuess
            last=True

            # display message for wrong guesses and update the value of check as wrong
            if(wrong==(len(chooseWord))):
                print("Sorry your guess is wrong!")
                check="wrong"

            # update the value of check as done if there is correct guess(es)
            else:
                check="done"

            # call function to update the string in currentStr for display
            updateString(guessList,wrongGuess,newGuess,chooseWord,count,check,last,currentStr,guessDisplay,y)

    # run this part if the guess is a word
    else:
        # run this part if the word newGuess matches the answer of this game
        if newGuess == chooseWord:
            # update the currentStr as the answer and print it
            currentStr = chooseWord
            printCurrentStr(currentStr, chooseWord)

            # add one to count and call function to print the message for winning the game
            count+1
            count=len(guessList)
            win(count,chooseWord)

            # call function to end the game by asking user want to start another game or quit
            end()

        # run this part if the word newGuess does not match the answer (chooseWord)
        else:
            # print message for wrong guesses
            print("Sorry your guess is wrong!")

            # update the value of check as wrong for calling function for hangman display in the next stage
            check="wrong"
            y=0

            # update the value of last as False and call function to update the string for display
            last=False
            updateString(guessList,wrongGuess,newGuess,chooseWord,count,check,last,currentStr,guessDisplay,y)


    
def checkRepeatGuess(currentStr,newGuess,guessList,chooseWord,count,wrongGuess,guessDisplay):
    """Check if the guess entered by user is repeated"""

    # set a new variable repeat with value 0
    # set a new variable repeatChecked with value False
    repeat=0
    repeatchecked=False

    # run this part if newGuess is an alphabet
    if (len(newGuess)==1):

        # check if newGuess is repeated by comparing every item in guessList
        for checkRepeat in guessList:

            # add one to variable repeat to indicate repetition on guesses
            if newGuess == checkRepeat:
                repeat+=1

        # if guess is repeated, call function to display error message and ask for new guess from user again
        if (repeat>0):
            repeatDisplay(newGuess)
            game(chooseWord,count,guessList,wrongGuess,currentStr,guessDisplay)

        # if newGuess is not repeated, update the value of repeatchecked as True, then call function for checking if the guess is correct or not
        else:
            repeatchecked=True
            checkGuess(newGuess,guessList,chooseWord,wrongGuess,count,currentStr,guessDisplay,repeatchecked)

    # run this part of newGuess is a word
    else:
        # check if newGuess is repeated by comparing every item in guessList
        for item in guessList:
            if newGuess == item:
                repeat+=1
        if (repeat>0):
            repeatDisplay(newGuess)
            game(chooseWord,count,guessList,wrongGuess,currentStr,guessDisplay)

        # if newGuess is not repeated, call function to check if it is correct or not
        else:
            repeatchecked=True
            checkGuess(newGuess,guessList,chooseWord,wrongGuess,count,currentStr,guessDisplay,repeatchecked)


        
        
def updateString(guessList,wrongGuess,newGuess,chooseWord,count,check,last,currentStr,guessDisplay,y):
    """Update the status of guess word after each guess"""

    # if the value of the variable check is "correct", update the corresponding letter position in the current string
    if check=="correct":
        currentStr[y]=newGuess

    # if the checking is finished and there is or are correct guess(es), print message for correct guess
    # update the guessList and print out the currentStr
    if (last==True)and(check=="done"):
        print("Correct guess!")
        updateguessDisplay(guessList)
        printCurrentStr(currentStr,chooseWord)

    # if it is a wrong guess, update the guessList and print out the currentStr
    if check=="wrong":
        updateguessDisplay(guessList)
        printCurrentStr(currentStr,chooseWord)

        # set demo as False and add one to variable wrong Guess for HangMan display
        demo=False
        wrongGuess+=1

        # call function to display the hang man
        hangMan(demo,wrongGuess,count,chooseWord)

        # add one to count and ask for new guesses if wrongGuess has not reached 7 (player will lose if 7 wrong guesses are made)
        count+=1
        if (wrongGuess<7):
            game(chooseWord,count,guessList,wrongGuess,currentStr,guessDisplay)

    # set a variable test with value True to check if the currentStr is empty
    test=True

    # update the value of test as False if there's any spaces in currentStr
    for checkempty in currentStr:
        if (checkempty==" "):
            test=False

    # run this part if checking is finished and there's at least one correct guess(es)
    if (last==True)and(check=="done"):

        # if there's at least one empty spaces in currentStr, ask for new guess again
        if(test==False):
            count+=1
            game(chooseWord,count,guessList,wrongGuess,currentStr,guessDisplay)

        # if there's no empty spaces in currentStr, function is called to display message as player won the game
        elif(test==True):
            count+=1
            win(count,chooseWord)
            end()



def end():
    """Called when the game is ended, ask if user want to start a new game or quit"""

    # ask if user want to start a new game or quit the game
    ask = input("Enter 1 to start a new game or enter 0 to quit:\n")

    # validate user input
    if (checkZeroOne(ask)==False):
        end()

    # run this part if user input is valid
    else:
        # go to game level selection to start a new game
        if ask =="1":
            Level()

        # call function to ask for user confirmation on quiting the game
        elif ask=="0":
            confirmQuit()


def win(count,chooseWord):
    """Called when player win the game"""

    # display messages if player won the game
    print("Bravo! The answer is: "+chooseWord+".")

    # show the number of guesses player used to make a correct guess
    print("You made the correct guess in "+str(count)+" times!\nCongradulations!\n")


def lose(count,chooseWord):
    """Called when user lose the game and display the correct answer of that game"""

    # display message when player lose
    print("Sorry you do not have any chances left, you lose!")

    # display the answer of the game
    answer=str(chooseWord)
    print("The answer is: "+answer+".")

    # call function to ask if user want to start a new game
    end()



## Display funcions ##

def hangMan(demo,wrongGuess,count,chooseWord):
    """Display the hangman for demo or after each wrong guess"""

    # globalize variable finish
    global finish

    # set a variable finish with value False to indicate when number of wrongGuess reached 7
    finish=False

    # build up a window using the graphics module for hang man display
    # identify every components of the hangman and named them with appropriate variables
    win = GraphWin("HangMan",500,500)
    box = Rectangle(Point(100,50),Point(400,400))
    box.draw(win)
    center = Point(250,150)
    neck = Point(250,200)
    lowercenter = Point(250,300)
    line = Line(Point(250,50),Point(250,100))
    head = Circle(center,50)
    lefthand = Line(Point(190,250),neck)
    righthand = Line(Point(310,250),neck)
    body = Line(lowercenter,neck)
    leftfeet = Line(Point(190,350),lowercenter)
    rightfeet = Line(Point(310,350),lowercenter)

    # add the text "Hang Man" in the window of hangman display
    text=Text(Point(250,450), "Hang Man")
    text.setStyle("bold italic")
    text.setSize(30)
    text.draw(win)

    # run this part if it is not a demo
    if demo==False:
        # determine the number of parts in the hangman need to be drawn base on the number of wrongGuess
        for a in range (wrongGuess):

            # draw the line
            if a ==0:
                line.draw(win)

            # draw the head
            if a ==1:
                head.draw(win)

            # draw the left hand
            if a ==2:
                lefthand.draw(win)

            # draw the right hand
            if a ==3:
                righthand.draw(win)

            # draw the body
            if a ==4:
                body.draw(win)

            # draw the left feet
            if a ==5:
                leftfeet.draw(win)

            # draw the right feet
            if a ==6:
                rightfeet.draw(win)
                # update the value of finish as True to indicate the player lose this game
                finish=True
                
    # run this part if it is a demo
    elif demo==True:
        # draw all the parts of the hang man for game demo
        line.draw(win)
        head.draw(win)
        lefthand.draw(win)
        righthand.draw(win)
        body.draw(win)
        leftfeet.draw(win)
        rightfeet.draw(win)

    # set a button for closing the hangman window by clicking it
    button=Text(Point(445,465), "Close")
    button.draw(win)
    r = Rectangle(Point(400,450), Point(490,480))
    r.draw(win)

    # window will be closed when user clicked within the area of the box with text "Close"
    for click in range(1000):
        c = win.getMouse()
        dx = c.getX()
        dy = c.getY()
        if((dx>=400)and(dx<=490))and((dy>=450)and(dy<=480)):
            win.close()
            break

    # call function to display message when player lose if the value of finish is True
    if finish==True:
        lose(count,chooseWord)



        
def printCurrentStr(currentStr,chooseWord):
    """Print the status of the guess word after each guess"""

    print("\n")

    # print every letter in the currentStr with spaces in between each of them
    for word in currentStr:
        print(word.rjust(2),end="")

    # print the underscores with formatting using the length of chooseWord
    underscore="- "*(len(chooseWord))
    print("\n",underscore)



def repeatDisplay(newGuess):
    """Called when user's guess is repeated"""

    # print message showing that user guess is repeated
    displayrepeat = "'"+newGuess+"'"
    print("You have tried",displayrepeat,"already!")



def updateguessDisplay(guessList):
    """Display all the guesses after each guess"""

    # display the updated guess list with comma and a space in between
    printDisplay = ", ".join(guessList)
    print("You have tried: ",end="")
    for displayGuessList in printDisplay:
        print(displayGuessList,end="")



## Main program code ##

if __name__=="__main__":

    # print welcoming message
    print("Welcome to the Hang Man game!\n")

    # display rules
    rules()

    # ask if user wants a demo
    Demo()

    # print ending message if user choose to quit
    if exit==True:
        print("See you next time, bye!")