"""
Create an adventure game where players can navigate through different locations.
The location descriptions are available in a dictionary with the location name as keys.
Each location have a determined number of exits and the location that exit will take you.

Players may write commands in different ways, ensure possible commands are understood by the program
 by creating another dictionary with the possible comamnds and their meaning.
 """
areas = {"room": {"description": ["You are in a messy, badly lit room. \nThe only source of light is a computer blinking on a command line and a door. \
            \nThere is another door in the room, closed.", "You are back in the bedroom."], "exits": {"w": "bathroom", "s": "living room"}},
            "bathroom": {"description": ["It stinks in here. You can identify a toilet and what looks like a bathtub. \
            \nThe bathtub is filled with dirty clothes.", "You are back in the bathroom."], "exits": {"e": "room"}},
            "living room": {"description": ["This room is nicer than the rest. Light comes in through bare windows on the walls south and east. \
            \nAn old couch sits in the middle of the otherwise empty room.", "You are back in the living room."], "exits": {"n": "room", "w": "kitchen", "s": "street"}},
            "kitchen": {"description": ["You find yourself in a kitchen. No dishes and no food on sight. \
            \nIt looks like the fridge has been turned off for some time.", "You are back in the kitchen."], "exits": {"e": "living room"}},
            "street": {"description": ["You leave the house into the street. The door locks behind you. \nCars are parked on the pavement, others on garages. \
            \nNo person in sight.", "You are back on the street."], "exits": {"e": "forest", "w": "mountains", "s": "beach"}},
            "mountains": {"description": ["You take one of the cars and follow the road uphill. \
            \nHigh pine trees are everywhere. The car starts making noises and loses speed. \nYou are out of petrol.", "You are back at the mountains."], "exits": {"s": "beach"}},
            "forest": {"description": ["You take one of the cars and follow the road east. \
            \nSoon the trees you could see every mile or so becomes more regular, until they turn into a forest. \
            \nThe paved road ends and becomes a dirt trail. The forest becomes denser with every mile. \
            \nYou are thinking of turning back, but then the car makes a noise and starts to lose speed. \
            \nSmoke fills the air, the car refuses to turn back on.", "You are back at the forest."], "exits": {"s": "beach"}},
            "beach": {"description": ["After walking for what seems to be an eternity, you start hearing a pleasant sound of waves. \
            \nThe sea spreads in front of you. You take off your shoes and feel the soft, warm sand under your feet. \
            \nA few cars seems to be parked at the coast.", "You are back on the beach."], "exits": {"e": "forest", "w": "mountains", "n": "street"}}}

movements = {"west": "w", "east": "e", "south": "s", "north": "n"}
history = ["room"]
current = "room"
print(areas[current]["description"][0])
while True:
    current_movements = []
    for e in movements:
        if movements[e] in areas[current]["exits"]:
            current_movements.append(e)
    area_exits = ", ".join(current_movements)
    direction = input("What do you do? ").lower()
    print("--------------------------------------------------\n" + direction.capitalize())
    if "quit" in direction.lower():
        print("Thank you for playing.")
        break
    word_check = 0
    for word in direction.split():
        if word == "help" or direction == "":
            word_check = 1
            print("You may go {0}. To quit, enter 'quit'.".format(area_exits))
        elif word in movements:
            word_check = 1
            new_current = areas[current]["exits"].get(movements[word])
            if new_current:
                current = new_current
                history.append(current)
                if history.count(current) > (len(areas[current]["description"]) - 1):
                    sit = len(areas[current]["description"]) - 1
                else:
                    sit = history.count(current) - 1
                print(areas[current]["description"][sit])
            else:
                print("You can't do that. You can go {0}.".format(area_exits))
    if word_check == 0:
        print("Sorry, you can't do that. You can go {0}.".format(area_exits))