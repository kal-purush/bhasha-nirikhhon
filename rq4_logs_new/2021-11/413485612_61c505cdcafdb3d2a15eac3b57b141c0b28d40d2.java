package chessAI;

import BoardManager.BoardManager;
import Controller.*;
import Command.*;

import java.awt.Point;

public class Agent {

	private Engine engine;
	private CommandSender cs;
	private BoardManager bm;

	public Agent(CommandSender cs, BoardManager bm) {
		engine = new GreedyButStupid(bm);
	}


	public void makeMove() {
		Point[2] decision = engine.getDecision();
		ChessMove newChessMove = cs.creatNewChessMove(decision[0].x, decision[0].y, decision[1].x, decision[1].y);
		cs.pressMove(newChessMove);
	}
}