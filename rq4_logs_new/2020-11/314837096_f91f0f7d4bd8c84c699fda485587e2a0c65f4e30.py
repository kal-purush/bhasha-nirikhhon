import random
from collections import deque
from colorama import Fore,Back,Style
import os

TOP=0
LEFT=1
BOTTOM=2
RIGHT=3

DIRECTION=[TOP,LEFT,BOTTOM,RIGHT]

class Maze:

    def __init__(self,rows,cols):

        self.rows=rows
        self.cols=cols

        self.mazeRows=rows*2+1
        self.mazeCols=cols*2+1

        self.grid=[[[True for direction in DIRECTION] for col in range(cols)] for row in range(rows)]

        self.visited=[] #list of visited cells
        self.positions = None #stack of cells to process

        self.maze=[]

        self.createPath((0,0))
        self.buildMaze()


    def isValid(self,position):
        row = position[0]
        col = position[1]
        if row < 0 or row >= self.rows: return False
        if col < 0 or col >= self.cols: return False
        return True

    def unvisitedNeighbours(self,position):
        row = position[0]
        col = position[1]
        neighbours = [ ( row, col - 1 ),( row, col + 1 ),( row+1, col ),( row-1, col )]
        return [ ( r , c ) for ( r , c ) in neighbours if self.isValid( ( r, c ) ) and ( r, c ) not in self.visited ]

    def updateWalls(self,fromCell,toCell):

        fromRow = fromCell[0]
        fromCol = fromCell[1]

        toRow = toCell[0]
        toCol = toCell[1]

        if fromRow < toRow:
            self.grid [ fromRow ][ fromCol ][ BOTTOM ] = False
            self.grid [ toRow ][ toCol ][ TOP ] = False

        elif fromRow > toRow:
            self.grid [ fromRow ] [ fromCol ] [ TOP ] = False
            self.grid [ toRow ] [ toCol ] [ BOTTOM ] = False

        elif fromCol > toCol:
            self.grid [fromRow][fromCol][LEFT]=False
            self.grid [toRow][toCol][RIGHT]=False

        elif fromCol < toCol:
            self.grid [ fromRow ] [ fromCol ] [ RIGHT ] = False
            self.grid [ toRow ] [ toCol ] [ LEFT ] = False

        else:

            print("fromCell==toCell: ", fromRow,fromCol,toRow,toCol) #should never happen

    def createPath(self,position):

        random.seed()

        maxVisited=self.rows*self.cols  #number of cells to be visited
        self.visited=[position]         #create empty list of visited cells
        self.positions=deque(position)  #create stack of cells to process
        previous=None                   #no previous cell process

        while len(self.visited) < maxVisited :

            if previous != None: self.updateWalls(previous,position)

            #keep track of visited cells

            if position not in self.visited:
                self.visited.append(position)

            # get a list of unvisited neighbours
            neighbours=self.unvisitedNeighbours(position)

            # get the next cell to process
            if neighbours:
                self.positions.append(position)  # update stack with parent cell
                previous=position                # current cell becoming previous
                position = random.choice(neighbours) #choose a random neigbour
            else:
                previous = None
                position = self.positions.pop()  # backtrack to previous


    def buildMaze(self):
        #init each cells as a wall
        self.maze=[[True for col in range(self.mazeCols)] for row in range(self.mazeRows)]

        # init the walkable cells (each walkable cell within 4 walls)
        for row in range(1,self.mazeRows,2):
            for col in range(1,self.mazeCols,2):
                self.maze[row][col]=False

        #Remove walls accordingly to the grid
        for row in range(self.rows):
            for col in range(self.cols):
                mazerow = row*2+1
                mazecol=col*2+1

                self.maze[mazerow][mazecol]=False #walkable cell


                if not self.grid[row][col][TOP]:
                    self.maze[mazerow-1][mazecol]=False  # remove top wall

                if not self.grid[row][col][BOTTOM]:
                    self.maze[mazerow+1][mazecol]=False   # remove bottom wall

                if not self.grid[row][col][RIGHT]:
                    self.maze[mazerow][mazecol+1]=False   # remove right wall

                if not self.grid[row][col][LEFT]:
                    self.maze[mazerow][mazecol-1]=False     #remove left wall

    def Draw(self,startpos,endpos):
        clear = lambda: os.system('clear')
        clear()

        print("\n\n")
        for row in range(self.mazeRows):
            line='    '
            for col in range(self.mazeCols):
                if (row,col)==(startpos):
                    line+=Back.GREEN+Fore.BLUE+'.'+Style.RESET_ALL  # cell is walkable
                elif (row,col)==endpos:
                    line+=Back.RED+Fore.BLUE+'.'+Style.RESET_ALL  # cell is walkable
                elif self.maze[row][col]:
                    line+=Back.BLACK+Fore.BLACK+'*'+Style.RESET_ALL # cell is not walkable
                else:
                    line+=Back.WHITE+Fore.BLUE+'.'+Style.RESET_ALL  # cell is walkable

            print(line)

        print('\n\n')


def inputSize():
    valid=False
    while True:
        try:
            rows=int(input('number of rows (without wall): '))
            cols=int(input('number of cols (without wall): ' ))
            return (rows,cols)
        except ValueError:
            print('Invalid number.  Please try again.')

def main():
    eot=False
    size=inputSize()
    startpos=(1,1)
    endpos=(size[0]*2+1-2,size[1]*2+1-2)

    while True:
        maze =Maze(size[0],size[1])
        maze.Draw(startpos,endpos)

        resp=input('[Enter] New maze, [C]hange size, [S]olve, S[a]ve, [Q]uit  ' ).lower()
        if resp=='q':
             break
        elif resp=='c':
            size=inputSize()
        elif resp=='a':
            f=open('maze.maze','w')
            f.write(f'{str(size[0])},{str(size[1])}\n')
            for row in range(len(maze.maze)):
                line = ''
                for col in range(len(maze.maze[0])):
                    if maze.maze[row][col]:
                        line += '1'
                    else:
                        line += '0'
                f.write(line+'\n')
            f.close()
            input("File saved maze.maze")
        else:
            pass
    #grid=generate((rows,cols),start)   #grid of each cell with indication of walls TOP, BOTTOM, RIGHT, LEFT


if __name__ == '__main__':
    main()