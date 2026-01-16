
morseStack = [
	['.','-'], #/*a*/
	['-','.','.','.'], #/*b*/
    ['-','.','-','.'], #/*c*/
	['-','.','.'], #/*d*/
	['.'], #/*e*/
	['.','.','-','.'],# /*f*/
	['-','-','.'], #/*g*/
	['.','.','.','.'],# /*h*/
	['.','.'], #/*i*/
	['.','-','-','-'],# /*j*/
	['-','.','-'], #/*k*/
	['.','-','.','.'], #/*l*/
	['-','-'], #/*m*/
	['-','.'], #/*n*/
	['-','-','-'], #/*o*/
	['.','-','-','.'],# /*p*/
	['-','-','.','-'], #/*q*/
	['.','-','.'], #/*r*/
	['.','.','.'], #/*s*/
	['-'], #/*t*/
	['.','.','-'], #/*u*/
	['.','.','.','-'], #/*v*/
	['.','-','-'], #/*w*/
	['-','.','.','-'], #/*x*/
	['-','.','-','-'], #/*y*/
	['-','-','.','.'], #/*z*/
]


class Morse():

	
    morseChars = []
    alphabet = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
    currentSym = 0

    def __init__(self):
        pass

    def getCurrentSym(self):
    	if self.currentSym > 0:
        	return self.morseChars[self.currentSym - 1]


    def incrementDot(self):
        self.morseChars.append('.')
        self.currentSym += 1

    def incrementDash(self):
        self.morseChars.append('-')
        self.currentSym += 1

    def getLetter(self):
        for i in range(len(morseStack)):
            listcopy = morseStack[i]
            if listcopy == self.morseChars:
                    return self.alphabet[i] #return the index of the alphabet

    def clearArray(self):
         self.morseChars = []
         self.currentSym = 0