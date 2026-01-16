package uk.co.icecreamhead.spoof.core.game;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: joshcooke
 * Date: 25/04/15
 * Time: 11:40
 */
public class Rules {
    private final List<String> players;
    private final int maxNumCoins;
    private final int numRounds;

    public Rules(List<String> players, int maxNumCoins, int numRounds) {
        this.players = players;
        this.maxNumCoins = maxNumCoins;
        this.numRounds = numRounds;
    }

    public List<String> getPlayers() {
        return players;
    }

    public int getMaxNumCoins() {
        return maxNumCoins;
    }

    public int getNumRounds() {
        return numRounds;
    }
}