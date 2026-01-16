public class TicTacToeGame {

	static  char[] board = new char[10];
	
	 //creating board char array and intialising it//
	 public static char[]  creatingBoard() {


		for (int i = 1; i < board.length; i++) {
			board[i] = ' ';
		}
		return board;

	}

	public static void main(String args[]) {

		System.out.println("Welcome to tic tac board");
		board = creatingBoard();
	}
}