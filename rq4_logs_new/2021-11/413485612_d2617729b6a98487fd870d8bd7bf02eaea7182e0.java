package TestPiece;

import org.junit.Before;
import org.junit.Test;
import piece.Color;
import piece.Pawn;
import piece.Rook;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestPieceDecorator {

    Pawn p;
    Rook r1;
    Rook r2;

    @Before
    public void before(){
        p = new Pawn("w_pawn_0", Color.WHITE);
        r1 = new Rook("w_rook_l", Color.WHITE);
        r2 = new Rook("b_rook_r", Color.BLACK);
    }

    @Test(timeout = 50)
    public void TestgetName(){
        assertEquals("w_pawn_0", p.getName());
    }

    @Test(timeout = 50)
    public void TestgetColor(){
        assertEquals(Color.WHITE, p.getColor());
    }

    @Test(timeout = 50)
    public void TestgetStatus(){
        assertFalse(p.getStatus());
    }

    @Test(timeout = 50)
    public void TestsetStatus(){
        p.setStatus(true);
        assertTrue(p.getStatus());
    }

    @Test(timeout = 50)
    public void TesthasSameColor(){
        assertTrue(p.hasSameColor(r1));
        assertFalse(p.hasSameColor(r2));
    }

}