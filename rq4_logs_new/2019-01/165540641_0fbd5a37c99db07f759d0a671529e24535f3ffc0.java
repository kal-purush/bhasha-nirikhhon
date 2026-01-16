import cs.ualberta.cmput402.tictactoe.scoreboard.Scoreboard;
import cs.ualberta.cmput402.tictactoe.board.Board.Player;

import org.junit.Before;
import org.junit.Test;
import org.junit.runners.model.TestClass;

public class ScoreboardTest {

    private Scoreboard scoreboard;

    @Before
    public void setup() {
        scoreboard = new Scoreboard();
    }

    @Test
    public void testInit() {
        assert(scoreboard.getPlayerWins(Player.X).equals(0));
        assert(scoreboard.getPlayerWins(Player.O).equals(0));
        assert(scoreboard.getTies().equals(0));
    }

    @Test
    public void testIncrement() {
        scoreboard.incrementPlayerWins(Player.X);
        assert(scoreboard.getPlayerWins(Player.X).equals(1));
        assert(scoreboard.getPlayerWins(Player.O).equals(0));
        assert(scoreboard.getTies().equals(0));

        scoreboard.incrementTies();
        assert(scoreboard.getPlayerWins(Player.X).equals(1));
        assert(scoreboard.getPlayerWins(Player.O).equals(0));
        assert(scoreboard.getTies().equals(1));

        for (int i = 0; i < 10; i++) {
            scoreboard.incrementPlayerWins(Player.O);
        }
        assert(scoreboard.getPlayerWins(Player.X).equals(1));
        assert(scoreboard.getPlayerWins(Player.O).equals(10));
        assert(scoreboard.getTies().equals(1));
    }

}