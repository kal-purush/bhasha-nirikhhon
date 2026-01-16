def pig_latin(word):

    """Given a word, return the Pig Latin version of the word. 

    Pig Latin Rules:
        1. If the word begins with a consonant (not a, e, i, o, u), 
           move the first letter to the end and add ‘ay’
        2. If the word begins with a vowel, add ‘yay’ to the end
    
    For example:

    >>> pig_latin('porcupines')
    'orcupinespay'

    >>> pig_latin('apple')
    'appleyay'
    
    """

    arr = word.split('')
    print(arr)
    vowels = ['a', 'e', 'i', 'o', 'u']
    if arr[0] in vowels:
        return ''.join(arr + ['yay'])
    print("Hello")
    print(arr)
    return ''.join(arr[1:] + [arr[0], 'ay'])

print(pig_latin('porcupines'))