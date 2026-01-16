from random import randrange
print ('<<<<<Kickass Dice Simulator>>>>>')
odpoved = input ('Roll the dice \n')

if odpoved == 'roll':
    number = randrange (0, 100)
    print ('You rolled', number)
else:
    print ('Type /roll/')