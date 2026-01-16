package uk.co.icecreamhead.spoof.client;

import uk.co.icecreamhead.spoof.core.message.Guess;
import uk.co.icecreamhead.spoof.core.message.NumCoins;
import uk.co.icecreamhead.spoof.core.player.Player;

/**
 * Created with IntelliJ IDEA.
 * User: joshcooke
 * Date: 03/04/15
 * Time: 15:58
 */
public class JoshPlayer implements Player {
    @Override
    public String getName() {
        return "Josh";
    }

    @Override
    public NumCoins chooseNumCoins() {
        return new NumCoins(0);
    }

    @Override
    public Guess announceGuess() {
        return new Guess(10);
    }

    @Override
    public void hearOtherGuess(String player, Guess guess) {

    }

    @Override
    public void winner(String winningPlayer) {

    }
}