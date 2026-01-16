package challenge.hackerearth.recursion;

import java.util.Scanner;

import org.junit.Assert;
import org.junit.Test;

import challenge.TestBase;

public class NQueensProblem extends TestBase {

	public void solution() {
		Scanner s = getScanner();
		
		int boardSize = s.nextInt();
		int numbersOfQueens = boardSize;
		
		int[][] board = new int[boardSize][boardSize];
		for (int i = 0; i < boardSize; i++) 
			for (int j = 0; j < boardSize; j++) 
				board[i][j] = 0;
		
		boolean result = nQueens(board, numbersOfQueens);
		
		System.out.println(result);
		for (int i = 0; i < boardSize; i++) { 
			for (int j = 0; j < boardSize; j++) 
				System.out.print(board[i][j] + " ");
			
			System.out.println();
		}
	}
	
	
	private boolean nQueens(int[][] board, int numbersOfQueens) {
		if (numbersOfQueens == 0) return true;
		
		for (int i = 0; i < board.length; i++) {
			for (int j = 0; j < board[0].length; j++) {
				
				if (isAttacked(i, j, board)) continue;
				
				board[i][j] = 1;
				if (nQueens(board, numbersOfQueens -1) == true)
					return true;
				
				board[i][j] = 0;
			}
		}
		return false;
	}


	public boolean isAttacked(int x, int y, int[][] board) {
		
		// checking for xth row
		for (int i = 0; i < board.length; i++) 
			if (board[x][i] == 1) return true;
		
		// checking for yth row
		for (int i = 0; i < board.length; i++) 
			if (board[i][y] == 1) return true;
		
		// up left
		int row = x -1;
		int col = y -1;
		while (row >= 0 && col >= 0) {
			if (board[row][col] == 1) return true;
			row--;
			col--;
		}
		
		// up right
		row = x - 1;
		col = y + 1;
		while (row >= 0 && col < board.length) {
			if (board[row][col] == 1) return true;
			row--;
			col++;
		}
		
		// down left
		row = x + 1;
		col = y - 1;
		while (row < board.length && col >= 0) {
			if (board[row][col] == 1) return true;
			row++;
			col--;
		}
		
		// down right
		row = x + 1;
		col = y + 1;
		while (row < board.length && col < board.length) {
			if (board[row][col] == 1) return true;
			row++;
			col++;
		}
		
		return false;
	}
	

	@Test
	public void test() {
		solution();
		
		String[] lines = getLines();
		Assert.assertEquals("true", lines[0]);
	}
}