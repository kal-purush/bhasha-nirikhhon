import prompt,predicate
import random
switch=prompt.for_bool('Switch when asked?')
trace=prompt.for_bool('Trace game?')
games_played =  prompt.for_int('Enter number of games to play',is_legal= predicate.is_positive)

count = 0
win = 0
lose = 0
while True:
    if trace==True:
        print('Game Played')
    count += 1
    if games_played != 0:
        prize=random.randint(1,3)
        contestant =random.randint(1,3)
        if trace==True:
            print(' Prize behind door',prize,'/ Contestant chooses door',contestant) 
        while True:
            exposed=random.randint(1,3)
            if exposed != prize and exposed != contestant:
                print('  trying to expose door',exposed)
                break
            else:
                if trace==True:
                    print('  trying to expose door',exposed)
        if trace==True:
            print(' Monty exposes door',exposed)
        
        if switch==True:
            while True:
                switched=random.randint(1,3)
                if switched != exposed and switched != contestant:
                    break                              
            if switched==prize:
                win+=1
                if trace==True:
                    print(' Player won; has won',win,'times')
            else:
                lose+=1
                if trace==True:
                    print(' Player lost; has won',win,'times')                            
        else:
            if contestant==prize:
                win+=1
                if trace==True:
                    print(' Player won; has won',win,'times')
            else:
                lose+=1
                if trace==True:
                    print(' Player lost; has won',win,'times')
    if count==games_played:
        break
print('')
print(games_played,'games played')
print('',win,'games won')
print('',lose,'games lost')
            