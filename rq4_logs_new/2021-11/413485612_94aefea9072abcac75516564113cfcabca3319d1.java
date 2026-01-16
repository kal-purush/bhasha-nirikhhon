package TestController;

import Board.Board;
import BoardManager.BoardManager;
import Command.ChessMove;
import Controller.BoardUpdater;
import Controller.CommandSender;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestCommandSender {

    CommandSender cs;

    @Before
    public void before(){
        cs = new CommandSender(true);   //classic chess
    }

    @Test(timeout = 50)
    public void TestgetBoardUpdater(){
        BoardUpdater bu = new BoardUpdater(cs.getBoardManager());
        assertEquals(bu.getBoardImage(), cs.getBoardUpdater().getBoardImage()); // TODO: fix the only slightly connected params
    }

//    @Test(timeout = 50)
//    public void TestcreateNewChessMove(){
//        ChessMove commandMove = cs.createNewChessMove(0, 0, 1, 1);
//        ChessMove cm = new ChessMove(cs.getBoardManager(),0, 0, 1, 1, 1);
//        System.out.println(cm);
//        assertEquals(cm, commandMove);
//    }

}