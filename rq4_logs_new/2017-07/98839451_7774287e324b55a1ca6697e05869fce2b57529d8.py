def anti_vowel(text):
    st = ""
    for n in text:
        if n not in 'aeiouAEIOU':   
            st = st + n
    return st

anti_vowel("HEy You!")