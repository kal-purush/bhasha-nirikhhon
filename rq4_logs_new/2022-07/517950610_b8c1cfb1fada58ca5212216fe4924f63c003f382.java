
import java.awt.AWTException;
import java.awt.Robot;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.net.MalformedURLException;
import java.util.concurrent.TimeUnit;
import methodes.GettingNumberOfScreen;

public class Sudoku {

	public static final int GRID_SIZE = 9;
	private static final int LEVEL = 1; // 1 : Facile, 2 : Moyen, 3 : Difficile, 4 : Expert, 5 : Diabolique
	private static int COUNT_TRY = 0;
	
	public static void main(String[] args) throws InterruptedException {
		
		try {
			Robot bot = new Robot();
			bot.mouseMove(1052, 281);    
	 	    bot.mousePress(InputEvent.BUTTON1_DOWN_MASK);
	 	    bot.mouseRelease(InputEvent.BUTTON1_DOWN_MASK);
	 	    Thread.sleep(1000);
	 	    bot.mouseMove(1052, 430+(44*LEVEL));    
	 	    bot.mousePress(InputEvent.BUTTON1_DOWN_MASK);
	 	    bot.mouseRelease(InputEvent.BUTTON1_DOWN_MASK);
	 	    Thread.sleep(2500);
		} catch (AWTException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
 	    
		
		int[][] board = {
				{0, 0, 0, 0, 0, 0, 0, 0, 0},
				{0, 0, 0, 0, 0, 0, 0, 0, 0},
				{0, 0, 0, 0, 0, 0, 0, 0, 0},
				{0, 0, 0, 0, 0, 0, 0, 0, 0},
				{0, 0, 0, 0, 0, 0, 0, 0, 0},
				{0, 0, 0, 0, 0, 0, 0, 0, 0},
				{0, 0, 0, 0, 0, 0, 0, 0, 0},
				{0, 0, 0, 0, 0, 0, 0, 0, 0},
				{0, 0, 0, 0, 0, 0, 0, 0, 0}
		};		
		
		try {
			board = GettingNumberOfScreen.getBoard();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		printBoard(board);
		long startTime = System.nanoTime();
		if (solveBoard(board)) {
		    System.out.println("Solved successfully!");
		    fillSudoku(board);
		}
		else {
		    System.out.println("Unsolvable board");
		}
		  
		printBoard(board);
		long endTime = System.nanoTime();
		long timeElapsed = endTime - startTime;
		System.out.println("Temps d'ex�cution en millisecondes: " + timeElapsed / 1000000);
		System.out.println("Nombre d'essaie : " + COUNT_TRY);
	  
	}
	
	
	private static void printBoard(int[][] board) {
	  for (int row = 0; row < GRID_SIZE; row++) {
	    if (row % 3 == 0 && row != 0) {
	      System.out.println("-----------");
	    }
	    for (int column = 0; column < GRID_SIZE; column++) {
	      if (column % 3 == 0 && column != 0) {
	        System.out.print("|");
	      }
	      System.out.print(board[row][column]);
	    }
	    System.out.println();
	  }
	  System.out.println();
	  System.out.println();
	}
	
	
	private static boolean isNumberInRow(int[][] board, int number, int row) {
	  for (int i = 0; i < GRID_SIZE; i++) {
	    if (board[row][i] == number) {
	      return true;
	    }
	  }
	  return false;
	}
	
	private static boolean isNumberInColumn(int[][] board, int number, int column) {
	  for (int i = 0; i < GRID_SIZE; i++) {
	    if (board[i][column] == number) {
	      return true;
	    }
	  }
	  return false;
	}
	
	private static boolean isNumberInBox(int[][] board, int number, int row, int column) {
	  int localBoxRow = row - row % 3;
	  int localBoxColumn = column - column % 3;
	  
	  for (int i = localBoxRow; i < localBoxRow + 3; i++) {
	    for (int j = localBoxColumn; j < localBoxColumn + 3; j++) {
	      if (board[i][j] == number) {
	        return true;
	      }
	    }
	  }
	  return false;
	}
	
	private static boolean isValidPlacement(int[][] board, int number, int row, int column) {
	  return !isNumberInRow(board, number, row) &&
	      !isNumberInColumn(board, number, column) &&
	      !isNumberInBox(board, number, row, column);
	}
	
	private static boolean solveBoard(int[][] board) throws InterruptedException {
	  for (int row = 0; row < GRID_SIZE; row++) {
	    for (int column = 0; column < GRID_SIZE; column++) {
	      if (board[row][column] == 0) {
	        for (int numberToTry = 1; numberToTry <= GRID_SIZE; numberToTry++) {
	          if (isValidPlacement(board, numberToTry, row, column)) {
	            board[row][column] = numberToTry;
	            COUNT_TRY++;
	            if (solveBoard(board)) {
	            	return true;
	            }
	            else {
	              board[row][column] = 0;
	            }
	          }
	        }
	        return false;
	      }
	    }
	  }
	  return true;
	}
	
	private static void fillSudoku(int[][] board) {
		try {
			for (int row = 0; row < GRID_SIZE; row++) {
				for (int column = 0; column < GRID_SIZE; column++) {
					int pixel_x = 362+((54/2)+(column*54));
			    	int pixel_y = 234+((54/2)+(row*54));
			    	int number = board[row][column];
					click(pixel_x, pixel_y, number);
				}
			}
		} catch (AWTException e) {
			e.printStackTrace();
		}
	}
	
	public static void click(int x, int y, int number) throws AWTException{
	   
	    try {
	    	Robot bot = new Robot();
	 	    bot.mouseMove(x, y);    
	 	    bot.mousePress(InputEvent.BUTTON1_DOWN_MASK);
	 	    bot.mouseRelease(InputEvent.BUTTON1_DOWN_MASK);
			Thread.sleep(10);
			int key = 48 + number;
			bot.keyPress(KeyEvent.VK_BACK_SPACE);
			bot.keyPress(key);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	    
	}
}
