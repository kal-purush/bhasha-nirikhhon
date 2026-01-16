package jttt.Core.Players;

import jttt.Core.Board.Board;
import jttt.Core.Board.Mark;

public class GUIHumanPlayer extends Player {
    private int nextMove;

    public GUIHumanPlayer(Mark mark) {
        super(mark, Type.GUI);
        nextMove = -1;
    }

    @Override
    public int getNextPosition(Board board) {
        int playedMove = -1;
        if(hasNewMove()){
            playedMove = nextMove;
            nextMove = -1;
        }
        return playedMove;
    }

    public boolean hasNewMove() {
        return nextMove != -1;
    }

    public void setNextUserMove(String displayPositionId) {
        this.nextMove = Integer.parseInt(displayPositionId);
    }
}