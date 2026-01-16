package is.ru.tictactoe;

import java.util.*;
import java.util.regex.Matcher;
//import is.ru.tictactoe.Board;



public class TictactoeConsole{

	private static int[] moves;

	public static void main(String[] args) {

		Board.initBoard();
		displayBoard();		

	}


	public static void displayBoard() {
		for (int i=0; i<3; i++) {
			for (int j=0; j<3; j++) {
				String toPrint = "1";
				if (Board.checkSquare(i,j) == 0 ) toPrint = " ";
				else if (Board.checkSquare(i,j) == 2 ) toPrint = "x";
				System.out.print( toPrint + " " );
			}
			System.out.println("");
		}
	}

}
