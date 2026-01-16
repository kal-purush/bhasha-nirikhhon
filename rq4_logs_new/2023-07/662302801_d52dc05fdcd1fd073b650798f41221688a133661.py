from random import choice

name = input("What is the adventureer's name?...")
age = input("How old are you?")

if int(age) > 1000:
    description = " an elder "
else:
    description = " a younger one "

# Game Intro
print("Welcome to Middle Earth," + name)
print("")
print(f"So you are {description} , {name}")
print("")

# Items
things = ["goblins", "mines", "chocolate", "rocks", "trees", "pipe"]
friends = ["Bilbo", "Gandalf", "Aragorn", "Galadriel", "Gimli", "Haldir", "Arwen", "Thranduil ", "Legolas", "celeborn", "Glorfindel", "Elrond"]
actions = ["slay", "kiss", "save", "marry", "rescue", "eat", "study", "talk with"]
places = ["Old Forrest", "Lothlórien", "Fangorn", "Beleriand", "Valinor"]

# Story
things = choice(things)
friend = choice(friends)
action = choice(actions)
place = choice(places)