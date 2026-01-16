package CommandFuture;

import BoardManager.BoardManager;
import BoardManager.SuperBoardManager;

public class AttackMove extends Move{

	public AttackMove(BoardManager newBM, int oldX, int oldY, int newX, int newY) {
		super(newBM, oldX, oldY, newX, newY);
	}

	@Override
	public void execute() {
		/**
		 * TODO implement it
		 */
	}

	@Override
	public void undo() {
		((SuperBoardManager) boardManager).deductOrAddHp(oldPosition[0], oldPosition[1], newPosition[0], newPosition[1],
				false);
	}
}