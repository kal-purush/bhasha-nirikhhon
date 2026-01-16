from ingest import *
import spacy
from spacy.symbols import nsubj, VERB, NOUN, PROPN

class EntityRelation:
    def __init__(self, noun, associatedVerbs):
        self.entity = noun
        self.actions = associatedVerbs
        
    def __str__(self):
        print(self.entity + " : " + str(self.actions))

def log(doc):
    for token in doc:
        print(token.text, token.lemma_, token.pos_, token.tag_, token.dep_,
                token.shape_, token.is_alpha, token.is_stop)

def getEntityRelations(sent):
    entities = {}
    doc = nlp(sent)
    for possible_subject in doc:
        if possible_subject.dep == nsubj:
            if(possible_subject.text not in entities):
                entities[possible_subject.text] = EntityRelation(possible_subject.text, [])
            if(possible_subject.head.pos == VERB):
                entities[possible_subject.text].actions.append(possible_subject.head)
            
    return entities

def isAnySentenceIrrelevant(story, nextChoice1, nextChoice2):
    subjects = {entity for entityRelationMap in story for entity in entityRelationMap}
    choice1Subjects = {entity for entity in nextChoice1}
    choice2Subjects = {entity for entity in nextChoice2}
    referredSubject1 = subjects.intersection(choice1Subjects)
    referredSubject2 = subjects.intersection(choice2Subjects)
    if(len(referredSubject1) > 0 and len(referredSubject2) == 0):
        return 1
    elif(len(referredSubject2) > 0 and len(referredSubject1) == 0):
        return 0
    else:
        return -1

def chooseRelevantEnding(data):
    sent1 = getEntityRelations(data.inputSentences[0])
    sent2 = getEntityRelations(data.inputSentences[1])
    sent3 = getEntityRelations(data.inputSentences[2])
    sent4 = getEntityRelations(data.inputSentences[3])
    nxtSent1 = getEntityRelations(data.nextSentence1)
    nxtSent2 = getEntityRelations(data.nextSentence2)
    return isAnySentenceIrrelevant([sent1,sent2,sent3,sent4], nxtSent1, nxtSent2)


if __name__ == "__main__":
    nlp = spacy.load('en_core_web_sm')
    data = getValidationData(tokenize=False)

    correctCnt0 = 0
    correctCnt1 = 0
    incorrectCnt0 = 0
    incorrectCnt1 = 0
    notSureCnt1 = 0
    notSureCnt0 = 0

    for datapoint in data:
        ans = chooseRelevantEnding(datapoint)
        if(ans == -1):
            if(datapoint.answer == 1):
                notSureCnt1 += 1
            else:
                notSureCnt0 += 0
        elif(ans == 1 and datapoint.answer == 1):
            correctCnt1 += 1
        elif(ans == 1 and datapoint.answer != 1):
            print(datapoint.inputSentences)
            print(datapoint.nextSentence1)
            print(datapoint.nextSentence2)
            chooseRelevantEnding(datapoint)
            incorrectCnt1 += 1
        elif(ans == 0 and datapoint.answer == 0):
            correctCnt0 += 1
        elif(ans == 0 and datapoint.answer != 0):
            incorrectCnt0 += 1

    print("correctCnt0 : ", correctCnt0)
    print("correctCnt1 : ", correctCnt1)
    print("incorrectCnt0 : ", incorrectCnt0)
    print("incorrectCnt1 : ", incorrectCnt1)
    print("notSureCnt1 : ", notSureCnt1)
    print("notSureCnt0 : ", notSureCnt0)

    correct = correctCnt0 + correctCnt1
    incorrect = incorrectCnt0 + incorrectCnt1

    print("accuracy: " , (correct /  (correct + incorrect + notSureCnt1 + notSureCnt0)))
    
    print("precision 1: " , (correctCnt1 /  (correctCnt1 + incorrectCnt1)))
    print("recall 1: " , (correctCnt1 /  (correctCnt1 + incorrectCnt1 + notSureCnt1)))
    print("precision 0: " , (correctCnt1 /  (correctCnt0 + incorrectCnt0)))
    print("recall 0: " , (correctCnt1 /  (correctCnt0 + incorrectCnt0 + notSureCnt0)))
    
