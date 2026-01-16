package com.groupthree.mancala.gameplay;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BoardTest {
    Board board;

    @BeforeEach
    void setUp() {
        board = new Board();
    }

    @AfterEach
    void tearDown() {
        board = null;
    }


    @Test
    void testBoardIsProperlyInitialized() {
        var gameRows = board.getGameRows();
        var topRow = gameRows[0];
        var bottomRow = gameRows[1];

        assertEquals(2, gameRows.length);
        assertEquals(6, topRow.length);
        assertEquals(6, bottomRow.length);

        for (var value : topRow) {
            assertNotNull(value);
            assertFalse(value.isEmpty());
        }
    }

    @Test
    void testPlayerOneTurnSetToTrueWhenBoardInitialized() {
        assertTrue(board.isPlayerOneTurn());
    }

    @Test
    void testMoveStonesThrowsExceptionWhenInvalidFromParamSupplied() {
        assertThrows(
                IllegalArgumentException.class, () -> board.moveStones(23)
        );
    }

    @Test
    void testMoveStones() {
        board.moveStones(5);
        var topRow = board.getGameRows()[0];
        var bottomRow = board.getGameRows()[1];
        var playerOneStore = board.getPlayer1Store();
        var playerTwoStore = board.getPlayer2Store();


        Hole hole1TopRow = topRow[0];
        Hole hole2TopRow = topRow[1];
        Hole hole3TopRow = topRow[2];
        Hole hole4TopRow = topRow[3];
        Hole hole5TopRow = topRow[4];
        Hole hole6TopRow = topRow[5];


        assertEquals(3, hole1TopRow.checkStoneCount());
        assertEquals(8, hole2TopRow.checkStoneCount());
        assertEquals(1, hole3TopRow.checkStoneCount());
        assertEquals(2, hole4TopRow.checkStoneCount());
        assertEquals(1, hole5TopRow.checkStoneCount());
        assertEquals(7, hole6TopRow.checkStoneCount());

        assertEquals(4, playerOneStore.getNumberOfStonesInStore());

        Hole hole1BottomRow = bottomRow[0];
        Hole hole2BottomRow = bottomRow[1];
        Hole hole3BottomRow = bottomRow[2];
        Hole hole4BottomRow = bottomRow[3];
        Hole hole5BottomRow = bottomRow[4];
        Hole hole6BottomRow = bottomRow[5];

        assertEquals(7, hole1BottomRow.checkStoneCount());
        assertEquals(1, hole2BottomRow.checkStoneCount());
        assertEquals(7, hole3BottomRow.checkStoneCount());
        assertEquals(0, hole4BottomRow.checkStoneCount());
        assertEquals(7, hole5BottomRow.checkStoneCount());
        assertEquals(0, hole6BottomRow.checkStoneCount());

        assertEquals(0, playerTwoStore.getNumberOfStonesInStore());

        assertTrue(board.isPlayerOneTurn());


    }
}