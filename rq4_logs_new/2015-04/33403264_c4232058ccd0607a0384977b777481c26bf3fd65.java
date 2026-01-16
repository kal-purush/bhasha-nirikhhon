package uk.co.icecreamhead.spoof.core.handler;

import uk.co.icecreamhead.spoof.core.message.*;

/**
 * Created with IntelliJ IDEA.
 * User: joshcooke
 * Date: 05/04/15
 * Time: 21:12
 */
public interface MessageHandler {
    public void handle(CoinRequest coinRequest);

    public void handle(NumCoins numCoins);

    public void handle(Registration registration);

    public void handle(RegistrationFailed registrationFailed);

    public void handle(RegistrationAccepted registrationAccepted);

    public void handle(Guess guess);
}