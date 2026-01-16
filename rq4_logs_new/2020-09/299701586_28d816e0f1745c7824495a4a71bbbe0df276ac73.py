from Maelstrom import Maelstrom
import matplotlib.pyplot as plt
import random
from tkinter import *
import sys


QUADRANT_DEFAULTS = ['A', 'B', 'C', 'D']
SECTOR_DEFAULTS = ['1', '2', '3', '4', '5', '6', '7', '8']

maelstrom = None
quadrant = ''
sector = ''
new_quadrant = 'A'
new_sector = '7'

gui = Tk()
perception_label = Label(gui)
montressor_label = Label(gui)
player_label = Label(gui)
player_move_label = Label(gui)
new_location = StringVar()

def perceptionCheck():

    check = ('Perception DC is: ' + str(maelstrom.getMontressorPerceptionCheck()))
    montressor_cords = ('Montressor is at: ' + maelstrom.getMontressorCoordinates())
    player_cords = ('Player Ship is at: ' + maelstrom.getPlayerCoordinates())

    perception_label.config(text=check)
    perception_label.grid(row=0,column=1)

    montressor_label.config(text=montressor_cords)
    montressor_label.grid(row=0,column=2)

    player_label.config(text=player_cords)
    player_label.grid(row=0,column=3)

def movePlayer():

    move_confirmation = maelstrom.movePlayerShip(new_quadrant, new_sector)
    player_move_label.config(text=move_confirmation)
    player_move_label.grid(row=1,column=2)

if __name__ == '__main__':


    # Statistical analysis of perception algorithm
    print(QUADRANT_DEFAULTS[:2])

    checks = []
    x=0
    while x < 500:
        quadrant = random.choice(QUADRANT_DEFAULTS[:2])
        sector = random.choice(SECTOR_DEFAULTS)
        maelstrom = Maelstrom(quadrant, sector)
        checks.append(maelstrom.getMontressorPerceptionCheck())
        x+=1
    plt.hist(checks, range(0,150))
    plt.show() 
    exit()

    """
    print('Montressor is at: ' + maelstrom.getMontressorCoordinates())
    print('Player Ship is at: ' + maelstrom.getPlayerCoordinates())
    print('Perception check is: ' + str(maelstrom.getMontressorPerceptionCheck()))
    """

    #gui.title('Maelstrom!')

    location_box = Entry(gui, width = 15, textvariable = new_location)

    coords = list(location_box.get())
    if len(coords) > 1:
        new_quadrant = coords[0]
        new_sector = coords[1]

    perception_button = Button(gui, width=30, text="Perception Check!", command=perceptionCheck)

    move_player_button = Button(gui, width=30, text="Move player!", command=movePlayer())

    perception_button.grid(row=0,column=0)
    move_player_button.grid(row=1,column=0)
    location_box.grid(row=1,column=1)

    gui.mainloop()