package com.example.study;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class ScoreManagerTest {

    @Before
    public void setUp() throws Exception {
        // Reset the singleton instance using reflection
        Field instanceField = ScoreManager.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        instanceField.set(null, null);
    }

    @Test
    public void testInitialScore() {
        ScoreManager scoreManager = ScoreManager.getInstance();
        assertEquals(0, scoreManager.getScore());
    }

    @Test
    public void testIncrementScore() {
        ScoreManager scoreManager = ScoreManager.getInstance();
        scoreManager.incrementScore();
        assertEquals(1, scoreManager.getScore());
    }

    @Test
    public void testIncrementScoreMultipleTimes() {
        ScoreManager scoreManager = ScoreManager.getInstance();
        for (int i = 0; i < 5; i++) {
            scoreManager.incrementScore();
        }
        assertEquals(5, scoreManager.getScore());
    }
}