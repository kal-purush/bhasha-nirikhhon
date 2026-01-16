package TestController;

import BoardManager.BoardManager;
import Controller.BoardUpdater;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestBoardUpdater {

    BoardManager bm;
    BoardUpdater bu;

    @Before
    public void before(){
        bu = new BoardUpdater(bm);
    }

    @Test(timeout = 50)
    public void TestupdateBoardImage(){
        String[][] boardImage = new String[8][8];
        assertEquals(boardImage, bu.getBoardImage());
    }

}