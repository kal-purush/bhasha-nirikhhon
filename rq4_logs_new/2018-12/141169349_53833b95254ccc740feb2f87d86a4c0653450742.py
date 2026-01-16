#%%
def firstNotRepeatingCharacter(s):
    char_dict = {}
    for char in list(s):
        if char in char_dict:
            char_dict[char] += 1
        else:
            char_dict[char] = 1
    
    for char in char_dict:
        if char_dict[char] == 1:
            return char
    return _ 

firstNotRepeatingCharacter('abababacbababab')