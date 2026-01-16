package flagquest.flagquest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.RuntimeEnvironment;
import org.robolectric.annotation.Config;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(RobolectricTestRunner.class)
public class ScoreDBHandlerTest {
    private ScoreDBHandler score;

    @Before
    public void SetUp() {
        score = new ScoreDBHandler(RuntimeEnvironment.application);
        score.clear();
    }

    @After
    public void TearDown() {
        score.clear();
    }

    @Test
    public void addScore() {
        score.addScore(new Score("Zeus", 1300, Calendar.getInstance().getTime(), 0));

        List<Score> scores = score.getAllScore(0);

        assertTrue(scores.size() == 1);
    }

    @Test
    public void getAllScore() {
        score.addScore(new Score("Zeus", 1300, Calendar.getInstance().getTime(), 0));
        score.addScore(new Score("Zeus", 1300, Calendar.getInstance().getTime(), 0));
        score.addScore(new Score("Zeus", 1300, Calendar.getInstance().getTime(), 0));

        List<Score> scores = score.getAllScore(0);

        assertTrue(scores.size() == 3);
    }

    @Test
    public void getHighscore() {
        score.addScore(new Score("Zeus", 1300, Calendar.getInstance().getTime(), 0));
        Score highscore = score.getHighscore(0);
        assertEquals(highscore.getScore(), 1300);
    }
}