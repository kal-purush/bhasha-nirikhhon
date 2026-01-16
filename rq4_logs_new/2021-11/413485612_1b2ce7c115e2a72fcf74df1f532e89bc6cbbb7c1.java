package chessAI;

import BoardManager.BoardManager;
import piece.PieceInterface;

import java.awt.Point;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Search algorithm for AI goes here
 *
 * We will likely implement a few different algorithms, they change AI's chess skill
 */
public abstract class Engine {
	protected State startingState;
	protected State bestState;
	protected PriorityQueue<State> searchingQueue;
	protected State currentState;

	protected int bestScore = -PieceInterface.KING_VALUE;
	protected int worstScore = PieceInterface.KING_VALUE;

	public Engine(BoardManager bm) {

		// TODO initialize states
		startingState = new State(bm.getBoard(), bm.getActivePlayer().getColor(), null);
		searchingQueue = new PriorityQueue<>(Comparator.comparingInt(State::getScore));
	}

	public abstract Point[] makeDecision();
}