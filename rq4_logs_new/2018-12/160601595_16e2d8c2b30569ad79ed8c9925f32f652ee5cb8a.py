"""

CP1404/CP5632 Practical

State names in a dictionary

File needs reformatting

"""

# TODO: Reformat this file so the dictionary code follows PEP 8 convention

STATE_NAMES = {"AliceBlue": "#f0f8ff", "AntiqueWhite": "#faebd7", "AntiqueWhite1": "#ffefdb", "AntiqueWhite2": "#eedfcc",
               "black": "#000000", "BlanchedAlmond": "#ffebcd", "blue1": "#0000ff"}

# print(STATE_NAMES)



color = input("Enter color name: ")

while color != "":
    if color in STATE_NAMES:

        print(color, "is", STATE_NAMES[color])

    else:

        print("Invalid short state")

    color = input("Enter short state: ")