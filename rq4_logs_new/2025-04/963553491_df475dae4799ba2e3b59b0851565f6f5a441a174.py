word = str
def is_palindrome(word: str) -> bool:
    j = 0
    g = -1
    if word == "":
        return True
    word = ''.join(char for char in word if char.isalpha())
    word = word.lower()
    while j < len(word):
        if word[j] == word[g]:
            j = j+1
            g = g-1
            return True
        else:
            j = len(word)
            return False