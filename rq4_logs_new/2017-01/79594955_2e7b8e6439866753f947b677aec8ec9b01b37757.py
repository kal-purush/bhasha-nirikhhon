import SymbolTable
import VMWriter
import Tokenizer
import os
import sys


"""
Compilation Enginge for EX11
"""

"""
Determines whether a value is in keywords array
"""


def checkIfTokenInArray(strippedToken):
    keywordsArray = ['let', 'var', 'do', 'while', 'if', 'return']
    if strippedToken in keywordsArray:
        return True
    return False


"""
Strip tags from token line
"""


def stripTags(taggedToken):
    token = taggedToken[taggedToken.index(">") + 1:taggedToken.rindex("<")]
    return token.strip()


"""
get type of Token
"""


def getTokenType(taggedToken):
    return taggedToken[taggedToken.index("<") + 1: taggedToken.index(">")]


def handleTabsArray(tabbedArray):
    return list(filter(lambda line: line != '\t', tabbedArray))


class Compiler:
    """represents the second stage of compiling a jack file to xm
       receives the Txml in array format and runs compile engine"""

    def __init__(self, tokenizedArray):
        self.tokenizedArray = tokenizedArray
        self.tokenizedArraySize = len(tokenizedArray)
        self.curIndex = 0
        self.indentLevel = 0
        self.compiledArray = []
        self.INDENT_SIZE = 2
        self.className = ''
        self.symbolsTable = SymbolTable.SymbolsTable()
        self.VMWriter = VMWriter.VMWriter()

    """
    boolean method for validating if token matches grammar
    """

    def checkMatchForToken(self, comparisonVal):

        curToken = stripTags(self.tokenizedArray[self.curIndex])
        if curToken == comparisonVal:
            return True
        else:
            return False

    """
    This is the only method that can advance the global index for current token
    """

    def advanceIndex(self):
        self.curIndex += 1
        return

    """
    Append title to scope in Xml array
    """

    def appendBlockTitle(self, isOpenBlock, blockTitle):
        whiteSpaceStr = " "
        if isOpenBlock:
            toAppend = whiteSpaceStr * self.indentLevel + "<" + blockTitle + ">\n"
            self.updateIndentLevel(True)
        else:
            self.updateIndentLevel(False)
            toAppend = whiteSpaceStr * self.indentLevel + "</" + blockTitle + ">\n"
        self.compiledArray.append(toAppend)
        return

    """
    Append tokenized line to Xml array
    """

    def appendTokenizedLine(self, tokenizedLine):
        whiteSpaceStr = " "
        self.compiledArray.append(whiteSpaceStr * self.indentLevel + tokenizedLine)
        return

    """
    increment or decrement indent level
    """

    def updateIndentLevel(self, toIncrement):
        if toIncrement:
            self.indentLevel += self.INDENT_SIZE
        else:
            self.indentLevel -= self.INDENT_SIZE
            if self.indentLevel < 0:
                self.indentLevel = 0
        return

    """
    get current token line
    """

    def getCurToken(self):
        return self.tokenizedArray[self.curIndex]

    """
    Peeks at next token
    """

    def peekNextToken(self):
        return stripTags(self.tokenizedArray[self.curIndex + 1])

    """
    Runs compile engine
    """

    def compileEngine(self):
        while self.curIndex < self.tokenizedArraySize:
            self.compileClass()
        return

    def compileClass(self):
        self.appendBlockTitle(True, "class")
        self.appendTokenizedLine(self.getCurToken())  #'class'
        self.advanceIndex()
        self.appendTokenizedLine(self.getCurToken())  #'className'
        self.advanceIndex()
        if not self.checkMatchForToken("{"):
            print("Error in compile class", self.curIndex)
            return
        self.appendTokenizedLine(self.getCurToken())  #'open curly brackets'
        self.advanceIndex()  #classVarDec or subRoutineDec
        while True:
            curToken = stripTags(self.getCurToken())
            if curToken == 'static' or curToken == 'field':
                self.compileVarDec(True)
            elif curToken == 'constructor' or curToken == 'function' or curToken == 'method':
                self.compileSubroutine()
            elif self.checkMatchForToken("}"):
                self.appendTokenizedLine(self.getCurToken())  #close curly brackets
                self.appendBlockTitle(False, "class")
                self.advanceIndex()
                return
            else:
                print("Error in compiling class", self.curIndex)
                return
        return

    def compileVarDec(self, isClassVarDec):
        if isClassVarDec:
            blockTitle = "classVarDec"
        else:
            blockTitle = "varDec"
        self.appendBlockTitle(True, blockTitle)
        self.appendTokenizedLine(self.getCurToken())   #static or field (identifier)
        self.advanceIndex()
        self.appendTokenizedLine(self.getCurToken())  #type (identifier) or keyword
        self.advanceIndex()
        self.appendTokenizedLine(self.getCurToken())  #varName (identifier)
        self.advanceIndex()
        if self.checkMatchForToken(';'):
            self.appendTokenizedLine(self.getCurToken())  #semicolon
            self.appendBlockTitle(False, blockTitle)
            self.advanceIndex()
            return
        while True:
            if self.checkMatchForToken(","):
                self.appendTokenizedLine(self.getCurToken())  #comma
                self.advanceIndex()
                if not getTokenType(self.getCurToken()) == "identifier":
                    print("Error in classVarDec", self.curIndex)
                    return
                self.appendTokenizedLine(self.getCurToken())  #varName
                self.advanceIndex()
            elif self.checkMatchForToken(";"):
                self.appendTokenizedLine(self.getCurToken())  #semicolon
                self.appendBlockTitle(False, blockTitle)
                self.advanceIndex()
                return
            else:
                print("Error in classVarDec", self.curIndex)
                return

        return

    def compileSubroutine(self):
        self.appendBlockTitle(True, "subroutineDec")
        self.appendTokenizedLine(self.getCurToken())  #const func method
        self.advanceIndex()
        self.appendTokenizedLine(self.getCurToken())  #void or type
        self.advanceIndex()
        if not getTokenType(self.getCurToken()) == "identifier":
            print("Error in compile subroutine", self.curIndex)
            return
        self.appendTokenizedLine(self.getCurToken())  #subroutineName
        self.advanceIndex()
        self.appendTokenizedLine(self.getCurToken())  #open brackets
        self.advanceIndex()
        self.compileParameterList()
        self.appendTokenizedLine(self.getCurToken())  # closing brackets
        self.advanceIndex()
        self.compileSubroutineBody()
        self.appendBlockTitle(False, "subroutineDec")
        return

    def compileSubroutineBody(self):
        self.appendBlockTitle(True, "subroutineBody")
        if not self.checkMatchForToken("{"):
            print("Error in compile subroutineBody", self.curIndex)
        self.appendTokenizedLine(self.getCurToken())  #open curly brackets
        self.advanceIndex()
        if self.checkMatchForToken("var"):
            while True:
                if self.checkMatchForToken("var"):
                    self.compileVarDec(False)
                else:
                    break
        self.compileStatements()
        if not self.checkMatchForToken("}"):
            print("Error in compile subroutineBody", self.curIndex)
        self.appendTokenizedLine(self.getCurToken())  #Closing curly brackets
        self.appendBlockTitle(False, "subroutineBody")
        self.advanceIndex()
        return

    def compileParameterList(self):
        self.appendBlockTitle(True, "parameterList")
        if self.checkMatchForToken(')'):
            self.appendBlockTitle(False, "parameterList")
            return
        self.appendTokenizedLine(self.getCurToken())  #type, parameter list not empty
        self.advanceIndex()
        if not getTokenType(self.getCurToken()) == "identifier":
            print("Error in compiling parameters list", self.curIndex)
        self.appendTokenizedLine(self.getCurToken())  #varName
        self.advanceIndex()
        while True:
            if self.checkMatchForToken(","):
                self.appendTokenizedLine(self.getCurToken())  #comma
                self.advanceIndex()
                self.appendTokenizedLine(self.getCurToken())  #type
                self.advanceIndex()
                if not getTokenType(self.getCurToken()) == "identifier":
                    print("Error in compiling parameters list", self.curIndex)
                    return
                self.appendTokenizedLine(self.getCurToken())  #varName
                self.advanceIndex()
            elif self.checkMatchForToken(")"):
                break
        self.appendBlockTitle(False, "parameterList")
        return

    def compileStatements(self):
        self.appendBlockTitle(True, "statements")
        while True:

            if self.checkMatchForToken("let"):
                self.compileLet()
            elif self.checkMatchForToken("if"):
                self.compileIf()
            elif self.checkMatchForToken("while"):
                self.compileWhile()
            elif self.checkMatchForToken("do"):
                self.compileDo()
            elif self.checkMatchForToken("return"):
                self.compileReturn()
            elif self.checkMatchForToken("var"):
                self.compileVarDec(False)
            elif self.checkMatchForToken("}"):
                self.appendBlockTitle(False, "statements")
                return
            else:
                print("Error in compile statements line ", self.curIndex)
                return
        return

    def compileDo(self):
        self.appendBlockTitle(True, "doStatement")
        if not self.checkMatchForToken('do'):
            print("Error in compile do ", self.curIndex)
            return
        self.appendTokenizedLine(self.getCurToken())  #'do'
        self.advanceIndex()
        self.compileSubroutineCall(self.peekNextToken())
        if not self.checkMatchForToken(";"):
            print("Error in compile do ", self.curIndex)
            return
        self.appendTokenizedLine(self.getCurToken())  #semicolon
        self.appendBlockTitle(False, "doStatement")
        self.advanceIndex()
        return

    def compileLet(self):
        self.appendBlockTitle(True, "letStatement")
        if not self.checkMatchForToken('let'):
            print("Error in compile let ", self.curIndex)
            return
        self.appendTokenizedLine(self.getCurToken())  #let
        self.advanceIndex()
        if not getTokenType(self.getCurToken()) == "identifier":
            print("Error in compile let ", self.curIndex)
        self.appendTokenizedLine(self.getCurToken())  #varName
        self.advanceIndex()
        if self.checkMatchForToken("["):
            self.appendTokenizedLine(self.getCurToken())  #open square brackets
            self.advanceIndex()
            self.compileExpression()
            if not self.checkMatchForToken("]"):
                print("Error in compile let ", self.curIndex)
                return
            self.appendTokenizedLine(self.getCurToken())  #close square brackets
            self.advanceIndex()
        if not self.checkMatchForToken("="):
            print("Error in compile let ", self.curIndex)
            return
        self.appendTokenizedLine(self.getCurToken())  #equal sign
        self.advanceIndex()
        self.compileExpression()
        if not self.checkMatchForToken(";"):
            print("Error in compile let ", self.curIndex)
            return
        self.appendTokenizedLine(self.getCurToken())  #semicolon
        self.appendBlockTitle(False, "letStatement")
        self.advanceIndex()
        return

    def compileWhile(self):

        self.appendBlockTitle(True, "whileStatement")
        if not self.checkMatchForToken("while"):
            print("Error in compile while", self.curIndex)
            return
        self.appendTokenizedLine(self.getCurToken())  #'while'
        self.advanceIndex()
        if not self.checkMatchForToken("("):
            print("Error in compile while", self.curIndex)
            return
        self.appendTokenizedLine(self.getCurToken())  #open brackets
        self.advanceIndex()
        self.compileExpression()
        if not self.checkMatchForToken(")"):
            print("Error in compile while", self.curIndex)
            return
        self.appendTokenizedLine(self.getCurToken())  #close brackets
        self.advanceIndex()
        if not self.checkMatchForToken("{"):
            print("Error in compile while", self.curIndex)
            return
        self.appendTokenizedLine(self.getCurToken())  #open curly brackets
        self.advanceIndex()
        self.compileStatements()
        if not self.checkMatchForToken("}"):
            print("Error in compile while", self.curIndex)
            return
        self.appendTokenizedLine(self.getCurToken())  #close curly brackets
        self.appendBlockTitle(False, "whileStatement")
        self.advanceIndex()
        return

    def compileReturn(self):
        self.appendBlockTitle(True, "returnStatement")
        if not self.checkMatchForToken("return"):
            print("Error in compile return", self.curIndex)
            return
        self.appendTokenizedLine(self.getCurToken())  #'return'
        self.advanceIndex()
        if not self.checkMatchForToken(";"):
            self.compileExpression()
        self.appendTokenizedLine(self.getCurToken())  #semicolon
        self.appendBlockTitle(False, "returnStatement")
        self.advanceIndex()
        return

    def compileIf(self):
        if not self.checkMatchForToken("if"):
            print("Error in compile if", self.curIndex)
            return
        self.appendBlockTitle(True, "ifStatement")
        self.appendTokenizedLine(self.getCurToken())  #if
        self.advanceIndex()
        if not self.checkMatchForToken("("):
            print("Error in compile if", self.curIndex)
            return
        self.appendTokenizedLine(self.getCurToken())  #open brackets
        self.advanceIndex()
        self.compileExpression()
        if not self.checkMatchForToken(")"):
            print("Error in compile if", self.curIndex)
            return
        self.appendTokenizedLine(self.getCurToken())  #close brackets
        self.advanceIndex()
        if not self.checkMatchForToken("{"):
            print("Error in compile if", self.curIndex)
            return
        self.appendTokenizedLine(self.getCurToken())  #open curly brackets
        self.advanceIndex()
        self.compileStatements()
        if not self.checkMatchForToken("}"):
            print("Error in compile if", self.curIndex)
            return
        self.appendTokenizedLine(self.getCurToken())  #close curly brackets
        self.advanceIndex()
        if self.checkMatchForToken("else"):
            self.appendTokenizedLine(self.getCurToken())  #'else'
            self.advanceIndex()
            if not self.checkMatchForToken("{"):
                print("Error in compile if", self.curIndex)
                return
            self.appendTokenizedLine(self.getCurToken())  #open curly brackets
            self.advanceIndex()
            self.compileStatements()
            if not self.checkMatchForToken("}"):
                print("Error in compile if", self.curIndex)
                return
            self.appendTokenizedLine(self.getCurToken())  #close curly brackets
            self.advanceIndex()
        self.appendBlockTitle(False, "ifStatement")
        return

    def compileExpression(self):
        opTable = ["+", "-", "*", '/', '&amp;', '|', '&lt;', '&gt;', "="]
        self.appendBlockTitle(True, "expression")
        self.compileTerm()
        if stripTags(self.getCurToken()) in opTable:
            while True:
                if stripTags(self.getCurToken()) in opTable:
                    self.appendTokenizedLine(self.getCurToken())  #op sign
                    self.advanceIndex()
                    self.compileTerm()
                else:
                    break
        self.appendBlockTitle(False, "expression")
        return

    def compileTerm(self):
        unaryOpTable = ['-', '~']
        keywordConstantsTable = ['true', 'false', 'null', 'this']
        self.appendBlockTitle(True, "term")
        if getTokenType(self.getCurToken()) == 'integerConstant' \
                or getTokenType(self.getCurToken()) == 'stringConstant':
            self.appendTokenizedLine(self.getCurToken())  #'constant'
            self.advanceIndex()
        elif stripTags(self.getCurToken()) in keywordConstantsTable:
            self.appendTokenizedLine(self.getCurToken())  #keyword constant
            self.advanceIndex()
        elif stripTags(self.getCurToken()) in unaryOpTable:
            self.appendTokenizedLine(self.getCurToken())  #unary op ter,
            self.advanceIndex()
            self.compileTerm()
        elif self.checkMatchForToken("("):
            self.appendTokenizedLine(self.getCurToken())  #open brackets
            self.advanceIndex()
            self.compileExpression()
            self.appendTokenizedLine(self.getCurToken())  #close brackets
            self.advanceIndex()
        elif getTokenType(self.getCurToken()) == "identifier":
            nextToken = self.peekNextToken()
            if nextToken == '.' or nextToken == '(':
                self.compileSubroutineCall(nextToken)
            elif nextToken == '[':
                self.appendTokenizedLine(self.getCurToken())  #var name
                self.advanceIndex()
                self.appendTokenizedLine(self.getCurToken())  #open square brackets
                self.advanceIndex()
                self.compileExpression()
                self.appendTokenizedLine(self.getCurToken())  #close square brackets
                self.advanceIndex()
            else:
                self.appendTokenizedLine(self.getCurToken())  #varName solo
                self.advanceIndex()
        self.appendBlockTitle(False, "term")
        return

    def compileExpressionList(self):
        self.appendBlockTitle(True, "expressionList")
        if self.checkMatchForToken(')'):
            self.appendBlockTitle(False, "expressionList")
            return

        self.compileExpression()
        while True:
            if self.checkMatchForToken(","):
                self.appendTokenizedLine(self.getCurToken())  #comma
                self.advanceIndex()
                self.compileExpression()
            else:
                self.appendBlockTitle(False, "expressionList")
                return
        return

    def compileSubroutineCall(self, nextToken):
        #No need for appending new scope for subroutine
        if nextToken == '(':
            self.appendTokenizedLine(self.getCurToken())  #subroutineName
            self.advanceIndex()
            self.appendTokenizedLine(self.getCurToken())  #open brackets
            self.advanceIndex()
            self.compileExpressionList()
            self.appendTokenizedLine(self.getCurToken())  #close brackets
            self.advanceIndex()
        elif nextToken == '.':
            self.appendTokenizedLine(self.getCurToken())  #classname or varName
            self.advanceIndex()
            self.appendTokenizedLine(self.getCurToken())  #dot
            self.advanceIndex()
            self.appendTokenizedLine(self.getCurToken())  #subroutineName
            self.advanceIndex()
            self.appendTokenizedLine(self.getCurToken())  #open brackets
            self.advanceIndex()
            self.compileExpressionList()
            self.appendTokenizedLine(self.getCurToken())  #closing brackets
            self.advanceIndex()
        else:
            print("Error in compile subroutineCall", self.curIndex)
            return

        return


def writeArrayToFile(arrayToWrite, fileNameStr, isTokenized):
    if isTokenized:
        arrayToWrite.insert(0, '<tokens>\n')
        arrayToWrite.insert(len(arrayToWrite), "</tokens>\n")
        fileNameStr = fileNameStr.replace(".xml", "T.xml")
    with open(fileNameStr, 'w') as outFile:
        for line in arrayToWrite:
            outFile.write(line)
    return


"""
Receives VMArray and a file name string with .vm suffix.
Writes the array to file
"""


def writeVMArrayToFile(VMarray, filenameStr):
    with open(filenameStr, 'w') as outFile:
        for line in VMarray:
            outFile.write(line)
    return


def main(argv):
    inputStr = argv[0]
    try:
        if os.path.isdir(inputStr):
            files = [file for file in os.listdir(inputStr) if file.endswith('.jack')]
            for file in files:
                tokenizedArray = Tokenizer.tokenizeFile(inputStr + '/' + file)
                compilerEng = Compiler(Compiler.handleTabsArray(tokenizedArray))
                compilerEng.compileEngine()
                outputFileName = file.replace(".jack", ".xml")
                outputStr = inputStr + '/' + outputFileName
                writeArrayToFile(compilerEng.compiledArray, outputStr, False)
                VMFileStr = inputStr + '/' + file.replace(".jack", "1.vm")
                writeVMArrayToFile(compilerEng.VMWriter.VMArray, VMFileStr)
        else:
            tokenizedArray = Tokenizer.tokenizeFile(inputStr)
            compilerEng = Compiler.Compiler(Compiler.handleTabsArray(tokenizedArray))
            compilerEng.compileEngine()
            outputFileName = inputStr.replace(".jack", ".xml")
            writeArrayToFile(compilerEng.compiledArray, outputFileName, False)
            VMFileStr = inputStr.replace(".jack", "1.vm")
            writeVMArrayToFile(compilerEng.VMWriter.VMArray, VMFileStr)
    except TypeError:
        print("I Love Nand")


if __name__ == "__main__":
    main(sys.argv[1:])