"""\
1) List
2) Create a Scorecard
3) Nested for loops to generate pattern
"""

"""
runs = 0

overs = 0.0
max_overs =1

wickets = 0
max_wickets = 10

current_bal_in_over = 0
no_of_balls_in_over='6'


types_of_runs = ['0', '1', '2', '3', '4', '5', '6']
types_of_extras = ['wd', 'nb']
types_of_wickets = ['lbw', 'b', 'catch', 'ro', 'stump', 'wicket']

print ("---------------------")
print(F"Types of runs: {',' .join(types_of_runs)}")
print(F"Types of wickets: {',' .join(types_of_wickets)}")

print(f'{runs}/{wickets}')
print(f'{overs}/{max_overs}')
print("---------------------")

while True :
    if wickets == 10 or overs == max_overs:
        break
    _input = str(input("Enter a run")).strip()
    if _input in types_of_extras:
        runs += 1
    elif _input in types_of_runs:
        runs += int[_input]
        current_bal_in_over +=1
    elif _input() in types_of_wickets:
        wickets +=1
        current_bal_in_over += 1
    else:
        print ("Wrong Input")
    if current_bal_in_over == no_of_balls_in_over:
        overs += 1
        current_bal_in_over =0

    print (F'{runs} / {wickets}')
    print (F'{overs}.{current_bal_in_over} ({max_overs})')
"""

'''
for i in range (0,5):
    for j in range (0,5):
        print("*", end=" ")
    print("")
'''

'''
for i in range (0,5):
    for j in range (0,5):
        print("1", end=" ")
    print("")
'''

'''
for i in range (0,5):
    for j in range (0,5):
        print(f"{i % 2}", end=" ")
    print("")
'''

''' (f)
for i in range (0,5):
    for j in range (1,6):
        print(f"{j}", end=" ")
    print("")
'''

'''(E)
for i in range (1,6):
    for j in range (1,6):
        print(f"{j % 2}", end=" ")
        if (i==j):
            break
    print("")
'''

'''(D)

for i in range (1,6):
    for j in range (1,6):
        if((i+j)%2 == 0):
            print(0, end="")
        else:
            print(1, end="")
        if (i==j):
            break
    print("")

'''
'''g
for i in range (0,5):
    for j in range (1,6):
        print("",5*i+j, end="")
    print("")

'''

''' (h)

for i in range(5,0,-1):
    for j in range(i,0,-1):
        if (i+j) % 2 == 0:
            print("1",end="")
        else:
            print("0", end="")
    print("")

'''
'''L)
for i in range(1,7):
    for j in range(1, i+1):
        print(2**j, end=" ")
    print("")

'''

'''
for i in range(4,-1,-1):
    space =" "*i
    print(space, end="")
    for j in range(5,i,-1):
        print("1",end=" ")
    print("")
'''

'''

for i in range(1, 4):
    for j in range(1,6):
        print(i*j, end=" ")
    print("")
    for k in range(5,0,-1):
        print(i*k, end=" ")
    print("")

'''

'''
for i in range (0,5):
    for j in range (0,i+1):
        print("*", end="")
    print("")
    for k in range(5, 0, -1):
        for l in range(1, k):
            print("*", end="")
        print("")

'''

for i in range(4,0,-1):
    space = " " * i
    print(space, end="")
    for j in range(0,9):
                









