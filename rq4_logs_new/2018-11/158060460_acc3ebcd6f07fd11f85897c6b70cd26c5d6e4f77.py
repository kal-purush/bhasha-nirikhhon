import models, printing, solver

def TranslateFromUserIndices(biqSquareIndex, smallSquareIndex):
    row = (biqSquareIndex // 3) * 3 + smallSquareIndex // 3
    col = (biqSquareIndex % 3) * 3 + smallSquareIndex % 3
    return row * 9 + col

sudoku = models.Sudoku()
printing.PrintSudoku(sudoku)

while True:
    userValue = input()

    if userValue == "q":
        break
    
    queue = []

    if len(userValue) == 3:
        queue.append([userValue[0], userValue[1], userValue[2]])
    
    if userValue.find(";") > -1:
        userValues = userValue.split(";")
        for uv in userValues:
            queue.append([uv[0], uv[1], uv[2]])

    while (len(queue) > 0):
        move = queue.pop(0)
        biqSquareIndex = int(move[0]) - 1
        smallSquareIndex = int(move[1]) - 1
        value = int(move[2])
        squareIndex = TranslateFromUserIndices(biqSquareIndex, smallSquareIndex)
        solver.Solve(sudoku, squareIndex, value)
        printing.PrintSudoku(sudoku)


    # Improvements:
    # 
    # Kolla igenom varje stor ruta, rad och kolumn om någon av rutorna är ensam om ett värde.
    #
    # When no more certain knockouts can be done, start trying different values out

    # Examples:
    #
    # Difficult sudoku in Dala-Demokraten 17/11 2018
    # 115;143;169;194;234;241;286;319;388;416;427;485;498;551;565;613;644;717;733;752;858;866;889;925
    #
    # Easy Dala-Demokraten 20/11 2018
    # 133;164;195;251;279;284;315;334;442;453;484;491;514;541;555;586;625;647;673;688;692;719;773;781;818;827;863;889;894;969;972;997

print("Quitting!")