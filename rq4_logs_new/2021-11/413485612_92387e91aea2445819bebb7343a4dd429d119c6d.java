package TestCommand;

import BoardManager.BoardManager;
import Command.ChessMove;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestChessMove {

    BoardManager bm;
    ChessMove cm;

    @Before
    public void before(){
        bm = new BoardManager();
        cm = new ChessMove(bm, 0, 0, 1, 1, 1);
    }

    @Test(timeout = 50)
    public void TestgetOldCoordX(){
        assertEquals(0, cm.getOldCoordX());
    }

    @Test(timeout = 50)
    public void TestgetOldCoordY(){
        assertEquals(0, cm.getOldCoordY());
    }

    @Test(timeout = 50)
    public void TestgetNewCoordX(){
        assertEquals(1, cm.getNewCoordX());
    }

    @Test(timeout = 50)
    public void TestgetNewCoordY(){
        assertEquals(1, cm.getNewCoordY());
    }

    @Test(timeout = 50)
    public void TestgetOldPieceName(){
        assertEquals("b_rook_l", cm.getOldPieceName());
    }

    @Test(timeout = 50)
    public void TestgetNewPieceName(){
        assertEquals("b_pawn_1", cm.getNewPieceName());
    }

    @Test(timeout = 50)
    public void TestgetMoveType(){
        assertEquals(1, cm.getMoveType());
    }

}