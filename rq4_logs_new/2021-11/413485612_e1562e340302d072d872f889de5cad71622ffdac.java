package TestCommand;

import BoardManager.BoardManager;
import Command.ChessMove;
import Command.Move;
import Command.MoveRecord;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class TestMoveRecord {

    BoardManager bm;
    ChessMove cm;
    MoveRecord mr;

    @Before
    public void before(){
        bm = new BoardManager();
        cm = new ChessMove(bm, 0, 0, 1, 1 ,1);
        mr = new MoveRecord();
    }

    @Test(timeout = 50)
    public void TestisEmpty(){
        assertTrue(mr.isEmpty());
    }

    @Test(timeout = 50)
    public void Testadd(){
        mr.add(cm);
        assertFalse(mr.isEmpty());
    }

    @Test(timeout = 50)
    public void Testremove(){
        mr.add(cm);
        assertFalse(mr.isEmpty());
        mr.remove();
        assertTrue(mr.isEmpty());
    }

    @Test(timeout = 50)
    public void Testget(){
        mr.add(cm);
        assertEquals(cm, mr.get());
    }

}