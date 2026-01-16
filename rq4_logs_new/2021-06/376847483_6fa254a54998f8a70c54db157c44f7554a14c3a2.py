import numpy as np
N=9

startingSudoku = """
                    006057820
                    210006750
                    753004009
                    960005300
                    000700402
                    300928006
                    020540001
                    001070948
                    009001070
                """

sudoku = np.array([[int(i) for i in line] for line in startingSudoku.split()])



def isExist(sudoku,ligne,colonne,num):
    for x in range(0,N):
        if sudoku[ligne,x]==num or sudoku[x,colonne]==num :
            return False
    debutLigne=ligne-ligne%3
    debutColonne=colonne-colonne%3
    for i in range(0,3):
        for j in range(0,3):
            if sudoku[i+debutLigne, j+debutColonne]==num :
                return False
    return True



def MethodeGloutonne(sudoku):
    for ligne in range(0,N):
        for colonne in range (0,N):
            if sudoku[ligne,colonne]!=0 : continue
            for num in range(1,N+1):
                if isExist(sudoku,ligne,colonne,num):
                    sudoku[ligne, colonne]=num
                    break
    return True;

def AfficherSudoku(sudoku):
    print("\n")
    for i in range(len(sudoku)):
        ligne = ""
        if i == 3 or i == 6:
            print("---------------------")
        for j in range(len(sudoku[i])):
            if j == 3 or j == 6:
                ligne += "| "
            ligne += str(sudoku[i,j])+" "
        print(ligne)

#solution = solveSudoku(sudoku)
AfficherSudoku(sudoku)
MethodeGloutonne(sudoku)
print("\n")
print("Le resultat est".center(50,'-'))
AfficherSudoku(sudoku)