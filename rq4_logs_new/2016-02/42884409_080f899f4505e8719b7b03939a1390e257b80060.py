

def diff1(word1, word2):
    count = sum([word1[i] != word2[i] for i in range(len(word1))])
    return True if count == 1 else False


def chain(string1, string2):
    listOfMatch = [[]]
    listOfWords = [line.strip() for line in open('wordlist.txt', 'r') if line.strip() != string1]
    chainr(string1, string2, [string1], listOfMatch, listOfWords)
    base = ""
    for lines in listOfMatch:
        base += " ".join(lines)+"\n"
    return base


def chainr(string1, string2, currMatch, listOfMatch, listOfWord):
    if string1 == string2:
        listOfMatch.append(currMatch)
    if len(listOfWord) > 0:
        for word in listOfWord:
            if diff1(word, string1):
                newList = [w for w in currMatch]
                listOfWordnew = [w for w in listOfWord if w != word]
                newList.append(word)
                chainr(word, string2, newList, listOfMatch, listOfWordnew)