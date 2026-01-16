from location import Location
from player_character import PlayerCharacter
from npc import Npc
from item import Item


class Cli:
    temp_loc ={}
    def welcome():
        name = input("Welcome to the Ramptops! We're happy to have you here, but what's your name traveller?")
        Cli.hello(name)
    
    def hello(name):
        print(f"Welcome {name}")

    #def check- where you are- check for allies and enemies and items--> give appropriate options

    def check():
        location = PlayerCharacter.current_location()
        if PlayerCharacter.current_location() == False:
            temp_loc ={0,0}
        else:
            Location.__repr__()
        Cli.checkLocation(location)
        
    
    def checkLocation(location):
        #check location for allies/enemies
            #if present let player know/give option to interact/fight
        #check location for items
            #if present let player interCact
        return ("Something")
    
    def options(): 
        choice = input("What would like to do? (Fight, Talk, Use Item, Pick up item, move)")
        #what do you want to do? take in command, check it is one of the correct words and do a thing based on it
        choice = choice.lower()
        # match choice:
        #     case move:
        #         return ("move")
        #     case fight:
        #         return ("fight")
        #     case talk:
        #         return ("talk")
        #     case use item:
        #         return ("use item")
        #     case pick up item:
        #         return ("pick up item")
        #     case:
        #         return ("try again")
        #should take in move, talk, use item, fight
        #stretch for choices to be given based on location