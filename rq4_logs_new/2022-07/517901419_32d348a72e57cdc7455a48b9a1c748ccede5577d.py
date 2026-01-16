import random
import string

pwLength = 16
alphaNum = (string.ascii_letters + string.digits + string.punctuation)
alphaLower = string.ascii_lowercase
alphaUpper = string.ascii_uppercase
alphaNum = string.digits
alphaSpecial = string.punctuation
newPw = []

def pathSelect():
    pick = random.randint(1,4)
    getNextChar = getRandomChar(pick)
    return getNextChar

def getRandomChar(x):
    switcher = {
        1: random.choice(alphaLower),
        2: random.choice(alphaUpper),
        3:random.choice(alphaNum),
        4: random.choice(alphaSpecial)
    }
    addToPW = switcher.get(x)
    newPw.append(addToPW)
    while len(newPw) < pwLength:
        pathSelect()

pathSelect()
print(newPw[0]+newPw[1]+newPw[2]+newPw[3]+newPw[4]+newPw[5]+newPw[6]+newPw[7]+newPw[8]+newPw[9]+newPw[10]+newPw[11]+newPw[12]+newPw[13]+newPw[14]+newPw[15])




