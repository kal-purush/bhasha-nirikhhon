from emojiDict import emojiLookup
import cPickle as pickle

def readThes():
    """Read in the Thesaurus """
    thes = {}
    f = open('../mthes/mobythes.aur', 'rU')
    for line in f.readlines():
        stuff = line.strip().split(',')
        thes[stuff[0]] = stuff[1:]
    f.close()
    return thes

def buildEmDict():
    """Build a Dictionary of words/phrases for emoji"""

    word2emoji={}
    emoDict = emojiLookup()
    thes = readThes()
    for key in emoDict.keys():
        inPhrases = key.split(', ')
        for phrase in inPhrases:
            if phrase in thes.keys():
                newWords = thes[phrase]
                newWords.append(phrase)
                for word in newWords:
                    if word in word2emoji.keys():
                        if emoDict[key] not in word2emoji[word]:
                            word2emoji[word].append(emoDict[key])
                    else:
                        word2emoji[word] = [emoDict[key]]

    return word2emoji

if __name__ == "__main__":

    word2emoji = buildEmDict()
    pickle.dump(word2emoji, open('word2emoji.p', 'wb'))