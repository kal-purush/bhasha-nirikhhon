package TestBoardManager;

import Board.Board;
import BoardManager.BoardManager;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBoardManager {
    BoardManager bm;
    Board b;

    @Before
    public void setup(){
        bm = new BoardManager();
        b = new Board(8, 8);
    }

//    @Test(timeout = 50)
//    public void TestgetBoard(){
//        assertTrue(Arrays.equals(b.getBoard(), bm.getCurrentBoard()));
//    }
    // TODO: fix this ^

    @Test(timeout = 50)
    public void TestgetCurrentBoard(){
        assertEquals(b.getBoard(), bm.getCurrentBoard());
    }

//    @Test(timeout = 50)
//    public void TestgetPieces(){
//
//        HashMap<String, PieceInterface> testMap = new HashMap<String, PieceInterface>();
//
//        testMap.put("w_rook_l", new Rook("w_rook_l", Color.WHITE));
//        testMap.put("w_knight_l", new Knight("w_knight_l", Color.WHITE));
//        testMap.put("w_bishop_l", new Bishop("w_bishop_l", Color.WHITE));
//        testMap.put("w_queen", new Queen("w_queen", Color.WHITE));
//        testMap.put("w_king", new King("w_king", Color.WHITE));
//        testMap.put("w_bishop_r", new Bishop("w_bishop_r", Color.WHITE));
//        testMap.put("w_knight_r", new Knight("w_knight_r", Color.WHITE));
//        testMap.put("w_rook_r", new Rook("w_rook_r", Color.WHITE));
//
//        for (int i = 0; i < 8; i++) {
//            String name = "w_pawn_" + i;
//            testMap.put(name, new Pawn(name, Color.WHITE));
//        }
//
//        testMap.put("b_rook_l", new Rook("b_rook_l", Color.BLACK));
//        testMap.put("b_knight_l", new Knight("b_knight_l", Color.BLACK));
//        testMap.put("b_bishop_l", new Bishop("b_bishop_l", Color.BLACK));
//        testMap.put("b_queen", new Queen("b_queen", Color.BLACK));
//        testMap.put("b_king", new King("b_king", Color.BLACK));
//        testMap.put("b_bishop_r", new Bishop("b_bishop_r", Color.BLACK));
//        testMap.put("b_knight_r", new Knight("b_knight_r", Color.BLACK));
//        testMap.put("b_rook_r", new Rook("b_rook_r", Color.BLACK));
//
//        for (int i = 0; i < 8; i++) {
//            String name = "b_pawn_" + i;
//            testMap.put(name, new Pawn(name, Color.BLACK));
//        }
//
//        System.out.println(testMap);
//        System.out.println(bm.getPieces());
//        assertTrue(testMap.equals(bm.getPieces()));
//
//    }
    // TODO: compare hashmaps where object addresses do not matter

//    @Test(timeout = 50)
//    public void TestgetActivePlayer(){
//        assertEquals(bm.getP1(), bm.getActivePlayer());
//    }

    @Test(timeout = 50)
    public void TestmovePiece(){
        assertEquals("vacant", bm.getBoard().getPiece(5, 0));
        bm.movePiece(6, 0, 5, 0);
        assertEquals("w_pawn_0", bm.getBoard().getPiece(5, 0));
    }

}