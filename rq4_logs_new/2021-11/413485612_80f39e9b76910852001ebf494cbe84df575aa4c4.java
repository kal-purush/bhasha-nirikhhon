package Command;

import BoardManager.BoardManager;
import piece.SuperPieceDecorator;

public class AttackMove extends Move {

	protected final int hpDeduction;

	public AttackMove(BoardManager bm, ChessMove cm) {
		super(bm, cm);
		hpDeduction = ((SuperPieceDecorator) actionPiece).getAtk();
	}

	/**
	 * Executes an attack. Add ChessMove to record in MoveRecord.
	 * Deduct target piece's hp by attacking piece's attack level.
	 */
	@Override
	public void execute() {
		BM.getMR().add(CM);
		int oldHp = ((SuperPieceDecorator) targetPiece).getHp();
		((SuperPieceDecorator) targetPiece).modifyHp(-hpDeduction);
		System.out.println("old Hp: " + oldHp + "     new Hp: " + ((SuperPieceDecorator) targetPiece).getHp());
	}

	/**
	 * Undo an attack by adding back target piece's hp. Remove this ChessMove from record in MoveRecord.
	 */
	@Override
	public void undo() {
		BM.getMR().remove();
		int oldHp = ((SuperPieceDecorator) targetPiece).getHp();
		((SuperPieceDecorator) targetPiece).modifyHp(hpDeduction);
		System.out.println("old Hp: " + oldHp + "     new Hp: " + ((SuperPieceDecorator) targetPiece).getHp());
	}
}