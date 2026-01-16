import time
import random


def print_pause(message_here):
    print(message_here)
    time.sleep(2)


def intro(planets):
    player_name = input("What is your name?\n")
    print_pause(f"Greetings, Commander {player_name}."
                "You have been tasked with the crucial mission of finding a\n"
                "sustainable planet to send " + planets + " popluation.")
    print_pause("After careful consideration, \nplanetary choices"
                " have been narrowed down to 2.")


def select_planet():
    while True:
        print_pause("1. The Planet ofNidori\n"
                    "2. The Planer of Fanok\n")
        planet = input("Please enter the planet you would like to visit.\n"
                       "Enter 1 for Nidori and 2 for Fanok\n")
        if planet == "1":
            print_pause("The coordinates for Nidori are set.")
            planet_1()
            break
        elif planet == "2":
            print_pause("The coordinates for Fanok are set.")
            planet_2()
            break
        else:
            print_pause("Sorry, I don't understand.Try again.")


def planet_1():
    print_pause("Looks like it will take at least 2 extra years")
    print_pause("of time we do not have to get there.")
    while True:
        warp_speed = input("Would you like to enable warp speed?"
                           "Enter 'y' for yes and 'n' for no.\n").lower()
        if warp_speed == "y":
            print_pause("Hang on for warp speed!")
            print_pause("Ready for take of in\n")
            print_pause("3")
            print_pause("2")
            print_pause("1")
            low_gas()
            break
        elif warp_speed == "n":
            print_pause("Taking the scenic route has its benefits.")
            astroid_shower()
            break
        else:
            print_pause("Sorry, I don't understand.")


def planet_2():
    print_pause("Oh no! It looks like we got hit by an astroid!!")
    while True:
        stay_put = input("Would you like to stop to assess the damage?"
                         "Enter 'y' for yes and 'n' for no.\n").lower()
        if stay_put == "y":
            print_pause("Congratulations, You made it to Fanok safe.\n"
                        "Celebrate with the locals.")
            mission_complete()
            break
        elif stay_put == "n":
            print_pause("You suffered too much damage and \n"
                        "are still far from your's destination.\n"
                        "You failed the mission.")
            new_game()
            break
        else:
            print_pause("Sorry, I don't understand.Try again.")


def low_gas():
    print_pause("Oh no! You have almost run out of fuel.")
    while True:
        use_gas = input("Do you want to send a message back to mission\n"
                        "control to request a re-fuel tank?\n"
                        "Type 'y' for yes and 'n' for no.\n").lower()
        if use_gas == "y":
            print_pause("Wonderful, Mission control has sent help!")
            print_pause("We are back on track!!")
            mission_complete()
            break
        elif use_gas == "n":
            print_pause("Oh no! You ran out of fuel!\n")
            print_pause("We are standed in space! Mission Failed!")
            new_game()
            break
        else:
            print_pause("Sorry, I don't understand.Try again.")


def astroid_shower():
    print_pause("Oh no, we got stuck in an astroid shower!")
    while True:
        damage = input("Would you like to stop to fix the damage?\n"
                       "Type 'y' for yes and 'n' for no.\n").lower()
        if damage == "y":
            low_gas()
            break
        elif damage == "n":
            print_pause("Ok, let's keep going!")
            mission_complete()
            break
        else:
            print_pause("Sorry, I don't understand.Try again.")


def new_game():
    while True:
        new_chance = input("Would you like another chance to\n"
                           "accept a new mission?\n"
                           "Type 'y' for yes and 'n' for no.\n").lower()
        if new_chance == "y":
            print_pause("Your new mission begins.")
            play_game()
            break
        elif new_chance == "n":
            print_pause("End Game!")
            exit()
        else:
            print_pause("Sorry, I dont understand. Try again.")


def mission_complete():
    print_pause("Oh look! You finally made it!")
    while True:
        destination = input("Would you like to land?\n"
                            "Type 'y' for yes and 'n' for no.\n").lower()
        if destination == "y":
            mission_complete()
            break
        elif destination == "n":
            print_pause("You failed the mission! Goodbye!")
            exit()
        else:
            print_pause("Sorry, I don't understand.Try again.")


def play_game():
    planets = random.choice(["Venus'", "Earth's", "Mars'", "Plutos"])
    intro(planets)
    select_planet()
    planet_1()
    planet_2()
    low_gas()
    mission_complete()
    new_game()


play_game()