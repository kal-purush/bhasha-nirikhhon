package com.TicTacToe;

public class TicTacToeGame 
{
	
	public static void main(String[] args) 
	{
		System.out.println("Welcome To Tic Toc Toe Game");

		TicTacToeGame ticTacGame = new TicTacToeGame();

		ticTacGame.printBoard();
	}

	public void printBoard() {
		char[] gameBoard =  new char[10];
		for (int i = 0; i < gameBoard.length; i++) 
		{
			gameBoard[i] = ' ';
			
		}

	}

}