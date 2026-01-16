import java.util.Scanner;

public class Main {
	
	private static Scanner scan = new Scanner(System.in);
	private static char player = 'X';
	private static char board[][] = new char[3][3];
	
	public static void main(String[] args) {
		for(int i = 0; i < board.length; i++) {
			for(int j = 0; j < board.length; j++) {
				board[i][j] = '_';
			}
		}
		
		
		play();
		
	}
	
	public static void printBoard() {
		for(int i = 0; i < board.length; i++) {
			for(int j = 0; j < board.length; j++) {
				if(j == 0) {
					System.out.print(" | ");
				}
				System.out.print(board[i][j] + " | ");
			}
			System.out.println();
		}
		
	}
	
	public static void play() {
		boolean playing = true;
		
		while(playing) {
			
			
			
			int sor = scan.nextInt()-1;
			int oszlop = scan.nextInt()-1;
			
			if(sor > 2 || oszlop > 2 || sor < 0 || oszlop < 0) {
				System.out.println("Invalid step!");
				playing = false;
			}
			
			board[sor][oszlop] = player;
			
			printBoard();
			
			if(gameEnds(sor, oszlop)) {
				playing = false;
				System.out.println("GAME OVER! Player " + player + " wins the game!");
			}
			
			
			if(player == 'X') {
				player = 'O';
			}
			else {
				player = 'X';
			}
			
		}
		
		
	}
	
	public static boolean gameEnds(int sor, int oszlop) {
		
		if(board[0][oszlop] == board[1][oszlop] && board[0][oszlop] == board[2][oszlop])
			return true;
		if(board[sor][0] == board[sor][1] && board[sor][0] == board[sor][2])
			return true;
		if(board [0][0] == board[1][1] && board[0][0] == board[2][2] && board[1][1] != '_')
			return true;
		if(board[0][2] == board[1][1] && board[0][2] == board[2][0] && board[1][1] != '_')
			return true;
		
		return false;
	}
	

}