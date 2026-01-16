package TestBoardManager;

import BoardManager.SuperBoardManager;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSuperBoardManager {

    SuperBoardManager sbm;

    @Before
    public void before(){
        sbm = new SuperBoardManager();
    }

    @Test(timeout = 50)
    public void TestgetHP(){
        assertEquals(4, sbm.getHp(0, 0));
    }

    @Test(timeout = 50)
    public void TestdeductOrAddHp(){
        sbm.movePiece(1, 0, 2, 0);
        sbm.movePiece(2, 0, 3, 0);
        sbm.movePiece(3, 0, 4, 0);
        sbm.movePiece(4, 0, 5, 0);
        sbm.movePiece(5, 0, 6, 0);
        sbm.movePiece(6, 0, 7, 0);
        sbm.movePiece(7, 0, 8, 0);
        sbm.movePiece(8, 0, 9, 0);
        sbm.movePiece(9, 0, 10, 0);
        sbm.deductOrAddHp(10, 0, 11, 0, true);
        sbm.movePiece(11, 0, 12, 0);
        sbm.movePiece(12, 2, 11, 0);
        sbm.deductOrAddHp(0, 0, 11, 0, true);
        assertEquals(2, sbm.getHp(11, 0));
    }

    @Test(timeout = 50)
    public void TestattackToDeath(){
        sbm.movePiece(1, 0, 2, 0);
        sbm.movePiece(2, 0, 3, 0);
        sbm.movePiece(3, 0, 4, 0);
        sbm.movePiece(4, 0, 5, 0);
        sbm.movePiece(5, 0, 6, 0);
        sbm.movePiece(6, 0, 7, 0);
        sbm.movePiece(7, 0, 8, 0);
        sbm.movePiece(8, 0, 9, 0);
        sbm.movePiece(9, 0, 10, 0);
        assertTrue(sbm.attackToDeath(10, 0, 11, 0));
    }

}