# An easy Sudoku Solver.

# A pretty print
def pprint(puzzle):
	for i in puzzle:
		print i

# Inital board
puzzle = [[5,3,0,0,7,0,0,0,0],
          [6,0,0,1,9,5,0,0,0],
          [0,9,8,0,0,0,0,6,0],
          [8,0,0,0,6,0,0,0,3],
          [4,0,0,8,0,3,0,0,1],
          [7,0,0,0,2,0,0,0,6],
          [0,6,0,0,0,0,2,8,0],
          [0,0,0,4,1,9,0,0,5],
          [0,0,0,0,8,0,0,7,9]]

# Solution of initial board
solution = [[5,3,4,6,7,8,9,1,2],
            [6,7,2,1,9,5,3,4,8],
            [1,9,8,3,4,2,5,6,7],
            [8,5,9,7,6,1,4,2,3],
            [4,2,6,8,5,3,7,9,1],
            [7,1,3,9,2,4,8,5,6],
            [9,6,1,5,3,7,2,8,4],
            [2,8,7,4,1,9,6,3,5],
            [3,4,5,2,8,6,1,7,9]]

# Find which numbers are no more allowed within the row
def get_row_found(puzzle, row):
	return [x for x in puzzle[row] if x]

# Find which numbers are no more allowed within the column
def get_col_found(puzzle, cell):		
	return [puzzle[row][cell] for row,x in enumerate(puzzle) if x]
	
# Find which numbers are no more allowed within the same sub-grid
def get_grid_found(puzzle, row, cell):	
	return [puzzle[(row//3)*3+r][(cell//3)*3+c] for r in range(3) for c in range(3) ]		

# Find how many numbers are unknown
def get_unknown(puzzle):
	return sum([ 1 for r in range(9) for x in puzzle[r] if not x])

# Just pause after you have checked all the lines
pause_after = False

# The actual Sudoku solver
from collections import defaultdict
def sudoku(puzzle):
	# keeping a dictionary for each cell options
	available = defaultdict(list)
	# to keep track of found cells...
	found = set()
	while get_unknown(puzzle)>0:
		for row in range(len(puzzle)):
			for cell in range(len(puzzle[row])):
				# if the cell has already a number in it		
				if puzzle[row][cell] != 0:
					available[row,cell] = []
					found.add((row,cell))
				# the cell is yet to be found	
				else:
					# elaborate the options of a cell considering what is already in the row, the column and the sub-grid
					available[row,cell] = [x for x in range(1,10) if x not in get_row_found(puzzle, row)+get_col_found(puzzle, cell)+get_grid_found(puzzle, row, cell)]
					# if the cell has only 1 option then solve it
					if len(available[row,cell]) == 1:
						puzzle[row][cell] = available[row,cell][0]
						found.add((row,cell))
				print row, cell, available[row,cell], ["","  <---   FounD!"][len(available[row,cell])==1]
		# when you have read the row, print the board situation with the new found cells
		pprint(puzzle)
		# and print how many cells are left to be found
		print "remaining: ",get_unknown(puzzle)
		if pause_after: raw_input("Press Enter to check next row...")	
	return puzzle

# Calling the function and printing the final solution		
pprint(sudoku(puzzle))
print puzzle == solution