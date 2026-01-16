package com.example.storm.habittracker;

import android.support.test.runner.AndroidJUnit4;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Date;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by Storm on 2016-10-02.
 */

@RunWith(AndroidJUnit4.class)
public class HabitCompletionTest {

    @Test
    public void toStringTest(){
        HabitCompletion habitCompletion = new HabitCompletion(1);
        Date completedDate = new Date();

        String resultString = "Completion #" + String.valueOf(1) + ": on " + completedDate.toString();

        //The strings should exactly equal each other
        assertTrue(resultString.equals(habitCompletion.toString()));

    }
    @Test
    public void getCompletedNumberTest(){
        HabitCompletion habitCompletion = new HabitCompletion(1);

        //we passed in 1, so that should be the completed number
        assertTrue(1 == habitCompletion.getCompletedNumber());
    }


}