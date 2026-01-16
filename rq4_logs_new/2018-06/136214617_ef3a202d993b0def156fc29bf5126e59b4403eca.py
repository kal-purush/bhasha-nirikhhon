#Pigify
#Turns words into Pig Latin Words

def first_vowel(w):
    """Returns:  position of the first vowel; -1 if no vowels.
    
    Precondition: w is a nonempty string with only lowercase letters"""

    first = -1  
    position = 0
    vowel = 'a' in w
    if vowel == True:
        position = w.index('a')
        first = w.index('a')
    vowel = 'e' in w
    if vowel == True:
        position = w.index('e')
        if position < first:
            first = position
        if first == -1:
            first = position
    vowel = 'i' in w
    if vowel == True:
        position = w.index('i')
        if position < first:
            first = position
        if first == -1:
            first = position
    vowel = 'o' in w
    if vowel == True:
        position = w.index('o')
        if position < first:
            first = position
        if first == -1:
            first = position
    vowel = 'u' in w
    if vowel == True:
        position = w.index('u')
        if position < first:
            first = position
        if first == -1:
            first = position
    vowel = 'y' in w
    if vowel == True:
        position = w.index('y')
        if position == 0:
            return first
        if position < first:
            first = position
        if first == -1:
            first = position
    return first
           
        


def Pigify(w):
    """returns: copy of w converted to Pig Latin.
    
    Precondition: w is a nonempty string with only lowercase letters"""
    
    start = first_vowel(w)
    front = w[start:]
    backend = start -1
    back = w[0:start]
    
    if start == 0:
        print (w + 'hay')
        return
        
    if w[0:1] == 'qu':
        print (w[2:] + 'quay')
        return
        
    print (front + back + 'ay')
    return
        
    
    
    
    