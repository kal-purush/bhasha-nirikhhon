import itertools

def is_square(word_list, sz):

    for i in range(sz):
        temp = ''
        word = word_list[i]
        for j in range(sz):
            temp = temp + word_list[j][i]
        if word!=temp:
            return False 

    return True 

def word_squares(words):

    if len(words[0])==1:
        return [[w] for w in words]

    sz = len(words[0])
    all = list(itertools.product(words, repeat = sz))
    res = []

    for l in all:
        if is_square(l, sz):
            res.append(l)

    return res

words = ["area","lead","wall","lady","ball"]
print(word_squares(words))

words = ["abat","baba","atan","atal"]
print(word_squares(words))