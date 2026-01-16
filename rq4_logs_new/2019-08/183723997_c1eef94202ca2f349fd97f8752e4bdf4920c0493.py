from sys import exit, stderr
import re
def debug(var, name="hoge"):
    print(name +":" + str(type(var)) + " = " + repr(var), file=stderr)
    return

SYMBOLS = {
    "R0":0,"R1":1,"R2":2,"R3":3,"R4":4,"R5":5,"R6":6,
    "R7":7,"R8":8,"R9":9,"R10":10,"R11":11,"R12":12,
    "R13":13,"R14":14,"R15":15,
    "SP":0,"LCL":1,"ARG":2,"THIS":3,"THAT":4,
    "SCREEN":16384,"KBD":24576
}
VARSTART = 16

COMPS = {
    "0":"0101010","1":"0111111","-1":"0111010",
    "D":"0001100","A":"0110000","M":"1110000",
    "!D":"0001101","!A":"0110001","!M":"1110001",
    "-D":"0001111","-A":"0110011","-M":"1110011",
    "D+1":"0011111","A+1":"0110111","M+1":"1110111",
    "D-1":"0001110","A-1":"0110010","M-1":"1110010",
    "D+A":"0000010","D+M":"1000010",
    "D-A":"0010011","D-M":"1010011",
    "A-D":"0000111","M-D":"1000111",
    "D&A":"0000000","D&M":"1000000",
    "D|A":"0010101","D|M":"1010101",
}

DESTS = {
    "N":"000","M":"001","D":"010",
    "MD":"011","A":"100","AM":"101",
    "AD":"110","AMD":"111",
}

JUMPS = {
    "N":"000","JGT":"001","JEQ":"010",
    "JGE":"011","JLT":"100","JNE":"101",
    "JLE":"110","JMP":"111",
}


varaddr = VARSTART
src = []
l = 0
while(True):
    try:
        S = str(input())
        S = list(re.split(r"\/\/",S))[0]
        S = "".join(re.split(r"[\s\t\n]",S))
        if len(S) == 0:
            continue
        # label Declaration
        if S[0] == "(":
            # debug(S[1:-1])
            SYMBOLS[S[1:-1]] = l
        else:
            src.append(S)
            l+=1
    except EOFError:
        break
for S in src:
    # A op
    if S[0] == "@":
        targ = S[1:]
        # 0xをカットしたい
        if targ.isnumeric():
            print(bin(int(targ))[2:].zfill(16))
        else:
            if targ not in SYMBOLS:
                SYMBOLS[targ] = varaddr
                varaddr += 1
            print(bin(SYMBOLS[targ])[2:].zfill(16))
    # C op
    else:
        # comp,dest領域の処理
        CD_J = S.split(";")
        # COMP(RHS)が左に来るようリバース
        operands = CD_J[0].split("=")[::-1]
        # comp
        C = COMPS[operands[0]]


        # dest
        if len(operands) == 1:
            D = DESTS["N"]
        else:
            D = DESTS[operands[1]]

        # jamp領域の処理
        if len(CD_J) > 1:
            J = JUMPS[CD_J[1]]
        else:
            J = JUMPS["N"]
        print("111" + C + D + J)
# print(SYMBOLS)