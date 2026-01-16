#ex36c.py lpthw
from random import *
from sys import exit
from monster import *

def house():
    hello = '''
    you come to the front door and it opens in front of you.  
    An old man is sitting in a corner.'''

    print hello
    greet = ['"Hello young stranger!', \
    '"My, my!  The good lord sent me a traveling man!',\
    '"You are a sight for sore eyes, handsome!']
    
    #    print choice(greet),
    print'''
    %s
    Come in, and if you can solve my riddle, 
    there will be a prize in it for you! Hahaha!"
    The old man has a sinister laugh, your skin crawls 
    as he rocks back and forth in his rocking chair, 
    his smile exposing his missing and decaying teeth.
    would you like to stay and solve the riddle? (yes,no)''' % choice(greet)
    answer = raw_input('> ')
    a = '''
    "Fine!  Riddle me this:
    The answer to this riddle is either True or False! 
    If something is not True or False, what is it"'''
    b ='''
    "Oh my!  You are the first to solve it!  Here is your prize!"
     '''
#might be cool to have a random python keyword test item as q's

    if answer == 'yes':
       print a
       riddle = raw_input("> ")

       if riddle == "False" :
           print b
           my_hero.potion()
           
       else:
           print "'Sorry, no prize for you! You may leave now!'"
           start()
       raw_input('\npress enter to return')
       start()

    if answer == 'no':
        print '''
    "Fine! Leave if you want to!
    I was growing tired of your company anyway!"
    says the old man spitting and waving his cane.
    '''
        start()

    else:
        house()

def woods():
    '''fight scenario'''
    my_mon = Monster(name = "Gorn", weapon ="flail",hp=10)
    print "A monster jumps in front of you"

    while True:
        print "\nWhat do you do? (attack, run, status, potion, help)"#add help
        option = raw_input("> ")

        if option == "attack":
            a = randint(1,10)

            if a <= 5:
                my_mon.attack()
                print "you've taken on damage!"
                my_hero.ouch()
#create player class to take damage and use potion 

            elif a >= 5:
               print "ouch!" 
               my_mon.ouch() 

        elif option == "run":
            print "Run away! Run away!"
            start()

        elif option == "help":
            my_hero.help()

        elif option =="status":
            print my_hero
            print my_mon
        elif option =="potion":
            print "you don't have any!"
        else:
            print "oops, not an option!"
            
def start():
    '''where program resumes after intro, top of tree structure.'''
    
    print '''
    You have two paths in front of you %s.
    On the left there is a wood and the path on the right leads to an old house
    Do you go down the right or left one? (right,left)
    ''' % my_hero.name 
    
    next = raw_input("> ")
    
    if next == 'left':
        woods()

    if next == 'right':
        house()

    else:
        print '''
    you wrote: %s.I asked for right or left!
    I'm a computer damn it, not a miracle worker!
        ''' % next
    exit(0)    
#possibility 3

print '''
    Hello!  Wecome to knights Realm!
    What is your name?
    '''
player = raw_input("> ")
my_hero = Hero(name=player,weapon='sword',items=[],hp=20) 
start()