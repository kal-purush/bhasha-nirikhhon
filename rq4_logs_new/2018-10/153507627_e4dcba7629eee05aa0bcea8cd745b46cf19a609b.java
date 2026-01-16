import game.pool.GameSession;
import game.pool.NewGameHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class GameTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(GameTest.class);

    @Test
    public void testConnectToGame() {
        for (int i = 0; i < 100; i++) {
            NewGameHandler.connectToGame("test");
        }
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            LOGGER.error("Exception was thrown while testing", e);
        }
        Assert.assertEquals(NewGameHandler.getGamePool().getGamePoolMap().size(), 11);
        NewGameHandler.clearGamePool();
    }

    @Test
    public void testGetGameId() {
        Assert.assertEquals(NewGameHandler.connectToGame("test"), GameSession.getAtomicLong().get() - 1);
        NewGameHandler.clearGamePool();
    }
}