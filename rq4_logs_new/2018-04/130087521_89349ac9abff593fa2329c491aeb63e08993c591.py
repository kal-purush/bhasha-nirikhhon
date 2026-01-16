#Cards will be tuples: rank 2-14 followed by suit, 
#spades: 1, hearts: 2 clubs: 3 diamonds: 4

def find_mode(x):
    z = list(set(x))
    occurrences = 1
    element = 0
    for i in z:
        if x.count(i) > occurrences:
            occurrences = x.count(i)
            element = i
            
    return element


def find_demode(x):
    z = list(set(x))
    occurrences = 3
    element = 0
    for i in z:
        if x.count(i) < occurrences:
            occurrences = x.count(i)
            element = i
            
    return element

def find_max_occurrences(x):
    z = list(set(x))
    occurrences = []
    for i in z:
        occurrences.append(x.count(i))
            
    return max(occurrences)    

def subsets(x):
    a = x[0] 
    b = x[1]
    c = x[2]
    d = x[3]
    e = x[4]
    f = x[5]
    g = x[6]
    
    return [
        [c, d, e, f, g],
        [b, d, e, f, g],
        [b, c, e, f, g],
        [b, c, d, f, g],
        [b, c, d, e, g],
        [b, c, d, e, f],
        [a, d, e, f, g],
        [a, c, e, f, g],
        [a, c, d, f, g],
        [a, c, d, e, g],

        [a, c, d, e, f],
        [a, b, e, f, g],
        [a, b, d, f, g],
        [a, b, d, e, g],
        [a, b, d, e, f],
        [a, b, c, f, g],
        [a, b, c, e, g],
        [a, b, c, e, f],
        [a, b, c, d, g],
        [a, b, c, d, f],

        [a, b, c, d, e]
        ]
        
    
def ranker(x, y):
    suits_x = []
    rankings_x = []
    suits_y = []
    rankings_y = []
    for i in range(5):
        suits_x.append((x[i])[1])
        rankings_x.append((x[i])[0])
    for i in range(5):
        suits_y.append((y[i])[1])
        rankings_y.append((y[i])[0])
        
    #check for 4-of-a-kind
    
    if find_max_occurrences(rankings_x) == 4 and find_max_occurrences(rankings_y) != 4:
            return 1
        
    elif find_max_occurrences(rankings_y) == 4 and find_max_occurrences(rankings_x) != 4:
            return -1
        
    elif find_max_occurrences(rankings_y) == 4 and find_max_occurrences(rankings_x) == 4: 
            
        if find_mode(rankings_y) < find_mode(rankings_x):
                return 1
            
        if find_mode(rankings_y) > find_mode(rankings_x):
                return -1 
            
        else:
                return 0
            
    #check for full house
    
    if (len(set(rankings_x)) == 2 and find_max_occurrences(rankings_x) == 3) and not (len(set(rankings_y)) == 2 and find_max_occurrences(rankings_y) == 3):
        return 1
    elif (len(set(rankings_y)) == 2 and find_max_occurrences(rankings_y) == 3) and not (len(set(rankings_x)) == 2 and find_max_occurrences(rankings_x) == 3):
        return -1    
    elif (len(set(rankings_y)) == 2 and find_max_occurrences(rankings_y) == 3) and (len(set(rankings_x)) == 2 and find_max_occurrences(rankings_x) == 3):
        
        if find_mode(rankings_y) > find_mode(rankings_x):
            return 1
                    
        if find_mode(rankings_y) < find_mode(rankings_x):
            return -1
                    
        else:
            return 0  
        
    
    #check for flush
    
    if len(set(suits_x)) == 1 and len(set(suits_y)) != 1:
        return 1
    elif len(set(suits_x)) != 1 and len(set(suits_y)) == 1:
        return -1
    elif len(set(suits_x)) == 1 and len(set(suits_y)) == 1:
        
        if max(rankings_x) > max(rankings_y):
            return 1
        elif max(rankings_y) > max(rankings_x):
            return -1
        else: 
            return 0
        
    #check for straight
    
    if (max(rankings_x)- min(rankings_x) == 4 and len(set(rankings_x)) == 5) and not (max(rankings_y)- min(rankings_y) == 4 and len(set(rankings_y)) == 5): 
        return 1
    elif not (max(rankings_x)- min(rankings_x) == 4 and len(set(rankings_x)) == 5) and (max(rankings_y)- min(rankings_y) == 4 and len(set(rankings_y)) == 5): 
        return -1
    elif (max(rankings_x)- min(rankings_x) == 4 and len(set(rankings_x)) == 5) and (max(rankings_y)- min(rankings_y) == 4 and len(set(rankings_y)) == 5): 
        
        if max(rankings_x) > max(rankings_y):
            return 1
        elif max(rankings_y) > max(rankings_x):
            return -1
        else: 
            return 0
        
    # check for triples
    
    if find_max_occurrences(rankings_x) == 3 and find_max_occurrences(rankings_y) != 3:
        return 1
    elif find_max_occurrences(rankings_x) != 3 and find_max_occurrences(rankings_y) == 3:
        return -1    
    elif find_max_occurrences(rankings_x) == 3 and find_max_occurrences(rankings_y) == 3:
        
        if find_mode(rankings_x) > find_mode(rankings_y):
            return 1
        elif find_mode(rankings_y) > find_mode(rankings_x):
            return -1
        else: 
            return 0
         
        
        
    #check for two pair
    
    if len(set(rankings_x)) == 3 and len(set(rankings_y)) != 3:
        return 1
    elif len(set(rankings_x)) != 3 and len(set(rankings_y)) == 3:
        return -1
    elif len(set(rankings_x)) == 3 and len(set(rankings_y)) == 3:
        
        rankings_x.remove(find_demode(rankings_x))
        rankings_y.remove(find_demode(rankings_y))
        
        if max(rankings_x) > max(rankings_y):
            return 1
        elif max(rankings_y) > max(rankings_x):
            return -1
        else:   
            rankings_y.remove(max(rankings_y))
            rankings_y.remove(max(rankings_y))
            rankings_x.remove(max(rankings_x))
            rankings_x.remove(max(rankings_x))
            
            if max(rankings_x) > max(rankings_y):
                return 1
            elif max(rankings_y) > max(rankings_x):
                return -1
            else: 
                return 0       
            
    #check for pair
    
    if find_max_occurrences(rankings_x) == 2 and find_max_occurrences(rankings_y) != 2:
        return 1
    elif find_max_occurrences(rankings_x) != 2 and find_max_occurrences(rankings_y) == 2:
        return -1    
    elif find_max_occurrences(rankings_x) == 2 and find_max_occurrences(rankings_y) == 3:
        
        if find_mode(rankings_x) > find_mode(rankings_y):
            return 1
        elif find_mode(rankings_y) > find_mode(rankings_x):
            return -1
        else: 
            return 0    
        
        
    #check for high card
        
    if max(rankings_x) > max(rankings_y):
        return 1
    elif max(rankings_y) > max(rankings_x):
        return -1
    else: 
        rankings_x.remove(max(rankings_x))
        rankings_y.remove(max(rankings_y))
        
        if max(rankings_x) > max(rankings_y):
            return 1
        elif max(rankings_y) > max(rankings_x):
            return -1 
        else:
            rankings_x.remove(max(rankings_x))
            rankings_y.remove(max(rankings_y))
            
            if max(rankings_x) > max(rankings_y):
                return 1
            elif max(rankings_y) > max(rankings_x):
                return -1 
            else:
                rankings_x.remove(max(rankings_x))
                rankings_y.remove(max(rankings_y))
                
                if max(rankings_x) > max(rankings_y):
                    return 1
                elif max(rankings_y) > max(rankings_x):
                    return -1 
                else:   
                    rankings_x.remove(max(rankings_x))
                    rankings_y.remove(max(rankings_y))
                    
                    if max(rankings_x) > max(rankings_y):
                        return 1    
                    elif max(rankings_y) > max(rankings_x):
                        return -1 
                    else:
                        return 0   



def seven_card_best(y):
    
    x = subsets(y)
    
    a = [x[1], x[1], x[0]][ranker(x[1], x[0])]
    b = [x[3], x[3], x[2]][ranker(x[3], x[2])]
    c = [x[5], x[5], x[4]][ranker(x[5], x[4])]
    d = [x[7], x[7], x[6]][ranker(x[7], x[6])]
    e = [x[9], x[9], x[8]][ranker(x[9], x[8])]
    f = [x[11], x[11], x[10]][ranker(x[11], x[10])]
    g = [x[13], x[13], x[12]][ranker(x[13], x[12])]
    h = [x[15], x[15], x[14]][ranker(x[15], x[14])]
    i = [x[17], x[17], x[16]][ranker(x[17], x[16])]
    j = [x[19], x[19], x[18]][ranker(x[19], x[18])]
    k = x[20]
    

    l = [a, a, b][ranker(a, b)]
    m = [c, c, d][ranker(c, d)]
    n = [e, e, f][ranker(e, f)]
    o = [g, g, h][ranker(g, h)]
    p = [i, i, j][ranker(i, j)]
    q = k
    
    r = [l, l, m][ranker(l, m)]
    s = [n, n, o][ranker(n, o)]
    t = [p, p, q][ranker(p, q)]
    
    u = [r, r, s][ranker(r, s)]
    
    v = [u, u, t][ranker(u, t)]
    
    return v
    
    
    
import random

hands = [[2, 1], [2, 2], [2, 3], [2, 4], [3, 1], [3, 2], [3, 3], [3, 4], [4, 1], [4, 2], [4, 3], [4, 4], [5, 1], [5, 2], [5, 3], [5, 4], [6, 1], [6, 2], [6, 3], [6, 4], [7, 1], [7, 2], [7, 3], [7, 4], [8, 1], [8, 2], [8, 3], [8, 4], [9, 1], [9, 2], [9, 3], [9, 4], [10, 1], [10, 2], [10, 3], [10, 4], [11, 1], [11, 2], [11, 3], [11, 4], [12, 1], [12, 2], [12, 3], [12, 4], [13, 1], [13, 2], [13, 3], [13, 4], [14, 1], [14, 2], [14, 3], [14, 4]]


def random_hand(thingamajigger):
    thingy = list(thingamajigger)
    
    a = thingy[random.randint(0, len(thingy)-1)]
    thingy.remove(a)
    b = thingy[random.randint(0, len(thingy)-2)]
    thingy.remove(b)
    c = thingy[random.randint(0, len(thingy)-3)]
    thingy.remove(c)
    d = thingy[random.randint(0, len(thingy)-4)]
    thingy.remove(d)
    e = thingy[random.randint(0, len(thingy)-5)]
    thingy.remove(e)
    f = thingy[random.randint(0, len(thingy)-6)]
    thingy.remove(f)
    g = thingy[random.randint(0, len(thingy)-7)]
    thingy.remove(g)
    h = thingy[random.randint(0, len(thingy)-8)]
  
    
    return [a, b, c, d, e, f, g, h]

"""
r = 0        
for i in range(1000000):
    x = random_hand()
    r = r + (1 + ranker([[4,2], [3, 3], [2, 1], [2, 1], [5, 1]], x))/2.0
    
    
print float(r)/1000000
"""
#set; start; how many

def cutter(x, y, z):
    output = []
    for i in range(z):
        output.append(x[y + i - 1])
    return output


def final_probability(our_hand, table):
    hands2 = list(hands)
    tablelen = len(table)
    r = 0
    for i in range(len(our_hand)):
        hands2.remove(our_hand[i])
    for i in range(len(table)):
        hands2.remove(table[i])
            
    for i in range(3000):
        randomcards = random_hand(hands2)
    
        personal_hand = list(our_hand) + list(table) + cutter(randomcards, 1, (5 - tablelen))
        their_hand = list(table) + cutter(randomcards, 1, (5 - tablelen)) + cutter(randomcards, (6 - tablelen), 2)
        
        r = r + (1 + ranker(seven_card_best(personal_hand), seven_card_best(their_hand)))/2.0
           
        
    return float(r)/3000
    
"""   
r = 0
for i in range(100000):
    x = random_hand(hands)
    r = r + (1 + ranker([[2, 1], [2, 1], [3, 1], [5, 1], [4, 2]], [x[1], x[2], x[3], x[4], x[5]]))/2.0

print 100 - r/1000.0
"""


def bet(pot, our_hand, table, people, dist):
    p = (final_probability(our_hand, table))
    print "Probability of beating a randomly chosen hand: " + str(p)
    p = p**people
    if people != 1:
        print "Probability of beating every single person: " + str(p)
    
    if p > 0.5:
        return "We have a better than 50% chance of winning. Bet as much as won't scare the others away."
    break_even = 101
    
    for i in range(101):
        if p*(pot + people*i) - ((1 - p) * i) < 0:
            break_even = i
            break 
        
    if break_even < 101:
        print str(break_even) + "$ will make us break even."
        
    else:
        print "The more the better. "
    
    if dist == 1:
        
        print "Here's the distribution: "
        for i in range(101):
            print i, int(p*(pot + people*i) - ((1 - p) * i))        

        
    
    
    
    
    
    