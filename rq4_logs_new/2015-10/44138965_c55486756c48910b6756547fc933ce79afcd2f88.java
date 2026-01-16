import gameLogic.GameState;
import gameLogic.GameStateManager;
import gui.GameGUI;

import javax.swing.JPanel;

import behavior.HumanWithConsole;
import behavior.MoveStrategy;


public class Game {
	JPanel gameBoard;
	boolean gameIsOver = false;
	GameGUI gui;
	GameStateManager gsm;
	MoveStrategy player1, player2, currentPlayer;
	
	public Game() {
		gui = new GameGUI();
		gsm = new GameStateManager(new GameState());
		player1 = new HumanWithConsole();
		player2 = new HumanWithConsole();
		currentPlayer = player1;
	}
	
	public void startGame() {
		while(!gameIsOver) {
			currentPlayer.makeMove(gsm);
			
			gameIsOver = gsm.checkWin();

			gui.render(gsm.getGameState());
	
			if (currentPlayer == player1) currentPlayer = player2;
			else currentPlayer = player1;
		}
	}
	
}