"""
Assignment: Selection Using Dictionary Assignment
Program: assign_level.py
Author: Colby Boell
Last date modified: 03/04/2022

The purpose of this program is to get user input for the difficulty level for the video game
and returns the proper amount of points that goes with the level.
"""


def switch_level(level_chosen):
    levels ={
        'N': 50,
        'B': 150,
        'E': 300,
        'A': 500}
    return levels.get(level_chosen, 'That Difficulty level does not exist')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('Choose the game difficulty:\nN for Novice\nB for Beginner\nE for Experienced\nA for Advanced')
    chosen_level = input('Choose a difficulty: ').upper()
    print(f'The points awarded for the chosen difficulty is: ', switch_level(chosen_level))

"""
Tests:
1.)
Choose a difficulty: E
result:
The points awarded for the chosen difficulty is:  300

2.)
Choose a difficulty: b
result:
The points awarded for the chosen difficulty is:  150
3.)
Choose a difficulty: k
result:
The points awarded for the chosen difficulty is:  That Difficulty level does not exist
4.)
Choose a difficulty: 45
result:
The points awarded for the chosen difficulty is:  That Difficulty level does not exist
"""