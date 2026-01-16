# Updated Animation Starter Code

from tkinter import *

####################################
# customize these functions
####################################

import random    
# The init function stores all the data into python.
# Each time init is called once at the beginning of the program
# data is a Struct, which can be given new data values using data.name = value
# data will be shared across all animation functions
def init(data):
    data.rows, data.cols, data.cellSize, data.margin = gameDimensions()
    board = []
    data.board = board
    for row in range(data.rows):
        row = []
        board.append(row)
        for col in range(data.cols):
            row.append("blue")
            
    data.fallingPiece = None
    data.fallingPieceColor = None
 # Seven "standard" pieces (tetrominoes)
    iPiece = [
        [  True,  True,  True,  True ]
    ]

    jPiece = [
        [  True, False, False ],
        [  True,  True,  True ]
    ]

    lPiece = [
        [ False, False,  True ],
        [  True,  True,  True ]
    ]

    oPiece = [
        [  True,  True ],
        [  True,  True ]
    ]

    sPiece = [
        [ False,  True,  True ],
        [  True,  True, False ]
    ]

    tPiece = [
        [ False,  True, False ],
        [  True,  True,  True ]
    ]

    zPiece = [
        [  True,  True, False ],
        [ False,  True,  True ]
    ]
    tetrisPieceColors = [ "red", "yellow", "magenta", "pink", "cyan", 
                            "green", "orange" ]
    tetrisPieces = [ iPiece, jPiece, lPiece, oPiece, sPiece, tPiece, zPiece ]
    data.tetrisPieceColors = tetrisPieceColors
    data.tetrisPieces = tetrisPieces
    data.fallingPieceRow = 0
    data.fallingPieceCol = 0
    newFallingPiece(data)
    data.paused = False
    data.selection = (-1, -1)
    data.end = False
    data.score = 0
    isGameOver = False
       
   
def newFallingPiece(data):
    board = data.board
    randomIndex = random.randint(0, len(data.tetrisPieces) - 1)
    fallingPiece = data.tetrisPieces[randomIndex]
    data.fallingPieceColor = data.tetrisPieceColors[randomIndex]
    data.fallingPieceRow = 0
    numFallingPieceCols = len(fallingPiece[0])
    data.fallingPieceCol = data.cols // 2 - numFallingPieceCols // 2
    data.fallingPiece = fallingPiece
    data.end = False
    
def drawFallingPiece(canvas, data):
    # print(data.fallingPiece)
    # fallingPieceCol = data.fallingPieceCol
    color = data.fallingPieceColor
    fallingPieceRow = data.fallingPieceRow
    fallingPieceCol = data.fallingPieceCol
    for row in range(len(data.fallingPiece)):
        for col in range(len(data.fallingPiece[0])):
            if data.fallingPiece[row][col] == True:
                drawCell(canvas, data, row + fallingPieceRow, col + fallingPieceCol, color)


def drawBoard(canvas,data):
    for row in range(data.rows):
        for col in range(data.cols):
            drawCell(canvas, data, row, col)

def drawCell(canvas, data, rows, cols, color = None):
    if color == None:
        color = data.board[rows][cols]
    canvas.create_rectangle(data.margin + cols * data.cellSize, 
                        data.margin + rows * data.cellSize,
                        data.margin + (cols + 1) * data.cellSize, 
                        data.margin + (rows + 1) * data.cellSize, 
                        fill = color)
                        
                        
               
def mousePressed(event, data):
    pass

# The event variable holds all of the data captured by the event loop
def keyPressed(event, data):
# If player presses direction keyboards, the game will move towards \
# that direction
    if event.keysym == "Left": 
    #(data, 0, -1) means col shifts to left by 1 cell
        moveFallingPiece(data, 0, -1) 
    elif event.keysym == "Right":
        moveFallingPiece(data, 0, 1)
    elif event.keysym == "Down":    
        moveFallingPiece(data, 1, 0)
    elif event.keysym == "Up":    
        rotateFallingPiece(data)
# If player presses "p" keyboard, the game will either pause or continue
    if event.char == 'p': 
        if data.paused == True: 
            data.paused = False
        else:
            data.paused = True
    elif event.char == 'r': 
        init(data)
# if the game ends
    if data.end == True:
        data.selection = (-1,-1)    
        
def moveFallingPiece(data, drow, dcol):
    if data.paused == False:
        newRow = drow + data.fallingPieceRow 
        newCol = dcol + data.fallingPieceCol
        data.fallingPieceCol = newCol
        data.fallingPieceRow = newRow
        if not fallingPieceIsLegal(data):
            newRow = data.fallingPieceRow - drow
            newCol = data.fallingPieceCol - dcol
            data.fallingPieceCol = newCol
            data.fallingPieceRow = newRow
            return False
        return True
        

def fallingPieceIsLegal(data):
    for row in range(len(data.fallingPiece)):
        for col in range(len(data.fallingPiece[0])):
            if data.fallingPiece[row][col] == True:
                cellRow = row + data.fallingPieceRow
                cellCol = col + data.fallingPieceCol
                if not (0 <= cellRow < data.rows) or\
                   not (0 <= cellCol < data.cols) or\
                   not (data.board[cellRow][cellCol] == "blue"):
                    return False
    return True
    
    
def rotateFallingPiece(data):
    if data.paused == False:
        oldNumRows = len(data.fallingPiece)
        oldNumCols = len(data.fallingPiece[0])
        newNumCols, newNumRows = oldNumRows, oldNumCols   
        oldFallingPiece = data.fallingPiece
    
        fallingPieceRow = data.fallingPieceRow
        fallingPieceCol = data.fallingPieceCol
        
        newFallingPiece = []
        for row in range(len(data.fallingPiece[0])):
            row = []
            for col in range(len(data.fallingPiece)):
                row.append(None) 
            newFallingPiece.append(row)
        
                
        for row in range(oldNumRows):
            for col in range(oldNumCols -1, -1, -1):
                newFallingPiece[len(newFallingPiece) - col - 1][row] = \
                                            data.fallingPiece[row][col]
                                        
        data.fallingPieceCol = data.fallingPieceCol + oldNumCols // 2 \
                                            - newNumCols // 2
        data.fallingPieceRow = data.fallingPieceRow + oldNumRows // 2 \
                                            - newNumRows // 2
        data.fallingPiece = newFallingPiece
        
        if not fallingPieceIsLegal(data):
            data.fallingPiece = oldFallingPiece
            data.fallingPieceRow = fallingPieceRow
            data.fallingPieceCol = fallingPieceCol


def placeFallingPiece(data):
    color = data.fallingPieceColor
    for row in range(len(data.fallingPiece)):
        for col in range(len(data.fallingPiece[0])):
            if data.fallingPiece[row][col] == True:
                data.board[row + data.fallingPieceRow][col + data.fallingPieceCol] = color
    removeFullRows(data)
    newFallingPiece(data)
    


def timerFired(data):
    if data.end == False:        
        if (data.paused or data.end): return
        else:    
            if moveFallingPiece(data,1,0) == True:
                pass
            else:
                placeFallingPiece(data)
                newFallingPiece(data)
                if fallingPieceIsLegal(data) == False:
                    data.end = True

def removeFullRows(data):
    newboard = []
    for row in range(len(data.board)):
        if 'blue' not in data.board[row]:
            change = True
            break
        else:
            change = False
            continue
            
    if change == True:
        for row in range(len(data.board)):
            for element in data.board[row]:
                if element == "blue":
                    newboard.append(data.board[row])
                    break
        addline = len(data.board)-len(newboard) 
        data.score += (len(data.board)-len(newboard)) ** 2
        lstAdd = []
        for row in range(addline):
            row = []
            lstAdd.append(row)
            for col in range(data.cols):
                row.append("blue")
        data.board = lstAdd + newboard

# This function computes graphics, and also the canvas will get \
# cleared and this will be called again constantly by the event loop
def redrawAll(canvas, data):
    canvas.create_rectangle(0, 0, data.width, data.height, fill = "orange")
    drawBoard(canvas, data)
    drawFallingPiece(canvas, data)
    youWon(canvas, data)
    drawScore(canvas, data)
# This function will print out contratulations if the player finishes it

def youWon(canvas, data):    
    if data.end == True:
        cx, cy = int(data.cols / 2), int(data.rows / 2)
        canvas.create_rectangle(data.margin, cy - data.margin, 
                                data.cols - data.margin, cy + data.margin, 
                                fill = "plum")
        canvas.create_text(cx, cy, text = "Game Over!", 
                                font = ("Arial", 50), fill = "indianred")    

def drawScore(canvas, data):
    canvas.create_text((data.width // 2), 0, 
                        text=("Score: " + str(int(data.score))),
                        anchor = N, font="Arial 20")
        
####################################
# use the run function as-is
####################################
def gameDimensions(rows = 15, cols = 10, cellSize = 20, margin = 25):
    return(rows, cols, cellSize, margin)
    
def playTetris():
    rows, cols, cellSize, margin = gameDimensions()
    width = 2 * margin + cols * cellSize
    height = 2 * margin + rows * cellSize
    
    run(width,height)
    
def run(width=300, height=300):
    def redrawAllWrapper(canvas, data):
        canvas.delete(ALL)
        canvas.create_rectangle(0, 0, data.width, data.height,
                                fill='white', width=0)
        redrawAll(canvas, data)
        canvas.update()    

    def mousePressedWrapper(event, canvas, data):
        mousePressed(event, data)
        redrawAllWrapper(canvas, data)

    def keyPressedWrapper(event, canvas, data):
        keyPressed(event, data)
        redrawAllWrapper(canvas, data)

    def timerFiredWrapper(canvas, data):
        timerFired(data)
        redrawAllWrapper(canvas, data)
        # pause, then call timerFired again
        canvas.after(data.timerDelay, timerFiredWrapper, canvas, data)
    # Set up data and call init
    class Struct(object): pass
    data = Struct()
    data.width = width
    data.height = height
    data.timerDelay = 500 # milliseconds
    root = Tk()
    root.resizable(width=False, height=False) # prevents resizing window
    init(data)
    # create the root and the canvas
    canvas = Canvas(root, width=data.width, height=data.height)
    canvas.configure(bd=0, highlightthickness=0)
    canvas.pack()
    # set up events
    root.bind("<Button-1>", lambda event:
                            mousePressedWrapper(event, canvas, data))
    root.bind("<Key>", lambda event:
                            keyPressedWrapper(event, canvas, data))
    timerFiredWrapper(canvas, data)
    # and launch the app
    root.mainloop()  # blocks until window is closed
    print("bye!")

playTetris()