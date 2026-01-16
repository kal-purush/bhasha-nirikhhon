def print_board(board):
	print ("The board is like this: \n")

	for i in range(3):
		print (" "),
		for j in range(3):
			if board[i*3+j] == 10:
				print ('X',end=" ")
			elif board[i*3+j] == 0:
				print ('O',end=" ")	
			elif board[i*3+j] != -1:
				print (board[i*3+j],end=" ")
			else:
				print (' ',end=" ")
			
			if j != 2:
				print (" | ",end="")
		print()
		
		if i != 2:
			print ("----------------")
		else: 
			print ()
			
def print_instruction():
	print ("Please use the following cell numbers to make your move")
	print_board([1,2,3,4,5,6,7,8,9])
         

def get_input(turn):
    

	valid = False
	while not valid:
		try:
			user = input("Where would you like to place " + turn + " (1-9)? ")
			user = int(user)
			if user >= 1 and user <= 9:
				return user-1
			else:
				print ("That is not a valid move! Please try again.")
				print_instruction()
		except Exception as e:
			print (user + " is not a valid move! Please try again.")
                 	
def check_win(board):
	win_cond = ((1,2,3),(4,5,6),(7,8,9),(1,4,7),(2,5,8),(3,6,9),(1,5,9),(3,5,7))
	for elem in win_cond:
		try:
			if board[elem[0]-1] == board[elem[1]-1] and board[elem[1]-1] == board[elem[2]-1]:
				return board[elem[0]-1]
		except:
			pass
	return -1

def quit_game(board,msg):
	print_board(board)
	print (msg)
	quit ()

def main():
	
	# setup game
	# alternate turns
	# check if win or end
	# quit and show the board
	
	print_instruction()

	board = []
	for i in range(9):
		board.append(-1)

	win = False
	move = 0
	while not win:

		# print board
		print_board(board)
		print ("Turn number " + str(move+1))
		if move % 2 == 0:
			turn = 'X'
			print ("it is player 1's turn")
		else:
			turn = 'O'
			print ("it is player 2's turn")

		# get user input
		user = get_input(turn)
		while board[user] != -1:
			print ("Invalid move! Cell already taken. Please try again.\n")
			user = get_input(turn)
		board[user] = 10 if turn == 'X' else 0

		# advance move and check for end game
		move += 1
		if move > 4:
			winner = check_win(board)
			if winner != -1:
				out = "The winner is " 
				out += "player 1" if winner == 10 else "player 2" 
				out += " congratulations :)"
				quit_game(board,out)
			elif move == 9:
				quit_game(board,"No winner :(")

main()