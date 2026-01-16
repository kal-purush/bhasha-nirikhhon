package is.ru.tictactoe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

public class BoardTest {

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.main("is.ru.tictactoe.BoardTest");
    }


    @Test
    public void winner(){
	assertEquals(true, Board.winner() );
    }


    @Test
    public void EmptyBoard(){
	//Tictactoe.Board board = new Tictactoe.Board();
	assertEquals(0, Board.checkSquare(0,0) );
    }


}