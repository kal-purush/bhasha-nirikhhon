# Detect if two strings are anagrams
# using lower case, same length string, 26 letter alphabet
def anagram_list(s1, s2):
    """use lists to check if two strings are anagrams"""
    # largely copied from exercise
    c1 = [0] * 26
    c2 = [0] * 26

    for i in range(len(s1)):
        pos = ord(s1[i]) - ord('a') # gets index
        c1[pos] = c1[pos] + 1

    # just in case diff lengths..
    for i in range(len(s2)):
        pos = ord(s2[i]) - ord('a')
        c2[pos] = c2[pos] + 1

    for i, ch in enumerate(c1): # not as clear but works
        if c2[i] != ch:         # two arrays with same char set
            return False

    return True

def anagram_dict(s1, s2):
    """same algorithm but using dict instead"""

    d1 = {}
    for ch in s1:
        d1.setdefault(ch, 0) # check current value or set to 0
        d1[ch] += 1

    d2 = {}
    for ch in s2:
        d2.setdefault(ch, 0)
        d2[ch] += 1

    if d1 == d2: # python is smart enough to do a shallow compare free
        return True
    else:
        return False

if __name__ == "__main__":
    print anagram_list('apple','pleap')
    print anagram_dict('apple','pleap')