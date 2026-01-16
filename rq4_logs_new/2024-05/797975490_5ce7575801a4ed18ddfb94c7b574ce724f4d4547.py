name = input('Type your name: \n')
print(f'Welcome {name} to this adventure!')

answer = input(
    'You are on a dirt road, it has come to an end and you can got left or right. Which way would you like to go? \n').lower()


if answer == 'left':
    answer = input('You come to river, you can walk around it or swim across. Choose walk or swim. \n').lower()
    if answer == 'swim':
        print('You swim across and got eaten by an alligator')
    elif answer == 'walk':
        print('You walk for many miles, ran out of water and you lost the game.')
    else:
        print('Not a valid option. You lose.')
elif answer == 'right':
    answer = input('You came to a bridge, it looks wobbly, do you want to cross it or head back? (cross/back) \n').lower()
    if answer == 'back':
        print('You go back and lose.')
    elif answer == 'cross':
        answer = input('You cross the bridge and meet a stranger, Do you talk to them? (yes/no) \n')
        if answer == 'yes':
            print('You talk to the stranger and they gave you gold. You WINNN...!!!')
        elif answer == 'no':
            print('You ignore the stranger and they shot you. You LOSE...!!!')
        else:
            print('Not a valid option. You lose.')
    else:
        print('Not a valid option. You lose.')
else:
    print('Not a valid option. You lose.')
print('Thanks for playing...\U0001f600')