package server;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GameTest {

    @Test
    public void testOrderPlayersByArray() {
        Player player1 = new Player("1", null);
        Player player2 = new Player("2", null);
        Player player3 = new Player("3", null);
        Player player4 = new Player("4", null);
        Player[] players = {player1, player2, player3, player4};

        int[] values = {3, 2, 6, 5};
        Player[] orderedPlayers = Game.orderPlayersByArray(players, values);
        Assertions.assertArrayEquals(orderedPlayers, new Player[]{player2, player1, player4, player3});
    }
}