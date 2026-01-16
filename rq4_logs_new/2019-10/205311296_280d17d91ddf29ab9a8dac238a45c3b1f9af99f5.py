from nltk.corpus import wordnet





#Get word from wn
def getWn(word):
    sysn = wordnet.synsets(word)
    return sysn


#Get its example sentence for first syn
def getExample(syn):
    word =syn[0]
    return word.examples()

#strip away an, the , is blah...


#get example sentences from those words


#make question  1. Which sentance makes sence a. I took a test yesterday b. The correct test is to first take out notebook... ect c. ...ect


if __name__ == '__main__':
    
    while True:
        
        word = input("enter word: ")
        
        if(word !='q'):

            syn = getWn(word)

            exam = getExample(syn)

            print(str(exam)+"\n")
        else: 
            break;