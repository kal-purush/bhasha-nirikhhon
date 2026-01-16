min, max = 138307, 654504
matches = 0

def match_A(x):
    '''6 digit number'''
    return x < 1000000

def match_B(x):
    '''Two adjacent digits are the same'''
    y = str(x)
    for i, c in enumerate(y[:-1]):
        if y[i+1] == c:
            return True
    
    return False

def match_C(x):
    '''Going from left to right, the digits never decrease'''
    y = str(x)
    for i, c in enumerate(y[:-1]):
        if int(c) > int(y[i+1]):
            return False
    
    return True

def match_D(x):
    '''Part 2 only pairs allowed'''
    # find all groups of matching chars
    # if len of any group is not % 2 = 0 
    # ret false
    y = str(x)
    groups = []       
    i = 0

    while(i < len(y) - 1):
        worker = ""
        for j, cc in enumerate(y[i:]):
            if cc != y[i]: break
            worker += cc
        if len(worker) >= 2: groups.append(worker)
        i+=j
    
    for w in groups:
        if len(w) % 2 != 0:
            return False
    
    return True


for i in range(min,max,1):
    if match_A(i) and match_B(i) and match_C(i):
        matches+=1

print(matches)
matches = 0

for i in range(min,max,1):
    if match_A(i) and match_B(i) and match_C(i) and match_D(i):
        matches+=1

print(matches)

# print(match_D(11442298922))