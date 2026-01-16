package CommandFuture;

import BoardManager.BoardManager;
import BoardManager.SuperBoardManager;


public class Move implements Command {

    private final int[] oldPosition = new int[2];
    private final int[] newPosition = new int[2];
    private BoardManager BM;
    private final String oldPieceName;
    private final String newPieceName;
    private int moveType;

    /**
     * @param typeOfMove   1 if it is an attack <P>
     *                     2 if it is a move
     *                     3 if it is a move after a successful attack
     */
    public Move(BoardManager newBM, int oldX, int oldY, int newX, int newY, int typeOfMove){
        BM = newBM;
        oldPosition[0] = oldX;
        oldPosition[1] = oldY;
        newPosition[0] = newX;
        newPosition[1] = newY;
        oldPieceName = newBM.getBoard().getPiece(oldX, oldY);
        newPieceName = newBM.getBoard().getPiece(newX, newY);
        moveType = typeOfMove;
    }

    public int getOldCoordX(){
        return oldPosition[0];
    }

    public int getOldCoordY(){
        return oldPosition[1];
    }

    public int getNewCoordX(){
        return newPosition[0];
    }

    public int getNewCoordY(){
        return newPosition[1];
    }

    public String getOldPieceName(){
        return oldPieceName;
    }

    public String getNewPieceName(){
        return newPieceName;
    }

    /**
     * @return  1 if it is an attack <P>
     *          2 if it is a move <P>
     *          3 if it is a move after a successful attack
     */
    public int getMoveType() {
        return moveType;
    }

    @Override
    public void execute() {
//        BM.getMR().add(this);
        
//        if (CM.getOldPieceName().contains("Pawn") ||
//                CM.getOldPieceName().contains("Rook") ||
//                CM.getOldPieceName().contains("King")  ){
//            BM.getPieces().get(CM.getOldPieceName());
//        }
        
        if (moveType > 1) {
            BM.movePiece(oldPosition[0], oldPosition[1], newPosition[0], newPosition[1]);
        }
    }
    
    @Override
    public void undo() {
        Move lastMove = BM.getMR().get();
        BM.getMR().remove();

        // move is an attack: add back health points
        if (lastMove.getMoveType() == 1){
            ((SuperBoardManager) BM).deductOrAddHp(lastMove.getOldCoordX(), lastMove.getOldCoordY(),
                    lastMove.getNewCoordX(), lastMove.getNewCoordY(), false);
        } // move is a move: put pieces back in place
        else {
            BM.movePiece(lastMove.getNewCoordX(), lastMove.getNewCoordY(), lastMove.getOldCoordX(), lastMove.getOldCoordY());
            BM.getBoard().addPiece(lastMove.getNewPieceName(), lastMove.getNewCoordX(), lastMove.getNewCoordY());
        } // move is a move after a successful attack: put pieces back in place and add back health points
        if (lastMove.getMoveType() == 3) {
            ((SuperBoardManager) BM).deductOrAddHp(lastMove.getOldCoordX(), lastMove.getOldCoordY(),
                    lastMove.getNewCoordX(), lastMove.getNewCoordY(), false);
        }
    }
}