package com.example.study;

import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;

import android.graphics.Color;

import androidx.test.espresso.Espresso;
import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.assertion.ViewAssertions;
import androidx.test.espresso.matcher.ViewMatchers;
import androidx.test.ext.junit.rules.ActivityScenarioRule;
import androidx.test.filters.LargeTest;
import androidx.test.internal.runner.junit4.AndroidJUnit4ClassRunner;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;


@RunWith(AndroidJUnit4ClassRunner.class)
@LargeTest
public class MathActivityTest {

    @Rule
    public ActivityScenarioRule<MathActivity> activityScenarioRule = new ActivityScenarioRule<>(MathActivity.class);
    @Test
    public void Correct(){
        Espresso.onView(withId(R.id.q1am)).perform(ViewActions.click());
    }



    @Test
    public void testScoreCalculation() {
        Espresso.onView(withId(R.id.q1bm)).perform(ViewActions.click());
        Espresso.onView(withId(R.id.q2bm)).perform(ViewActions.click());
        Espresso.onView(withId(R.id.q3cm)).perform(ViewActions.click());
        Espresso.onView(withId(R.id.submath)).perform(ViewActions.scrollTo(), ViewActions.click());
        Espresso.onView(withId(R.id.result_show))
                .check(ViewAssertions.matches(withText("Your Score: 3")));
    }
    @Test
    public void testAllAnswerOptionsClickable() {
        int[] buttonIds = {
                R.id.q1am, R.id.q1bm, R.id.q1cm, R.id.q1dm,
                R.id.q2am, R.id.q2bm, R.id.q2cm, R.id.q2dm,
                R.id.q3am, R.id.q3bm, R.id.q3cm, R.id.q3dm,
                R.id.q4am, R.id.q4bm, R.id.q4cm, R.id.q4dm,
                R.id.q5am, R.id.q5bm, R.id.q5cm, R.id.q5dm,
                R.id.q6am, R.id.q6bm, R.id.q6cm, R.id.q6dm,
                R.id.q7am, R.id.q7bm, R.id.q7cm, R.id.q7dm,
                R.id.q8am, R.id.q8bm, R.id.q8cm, R.id.q8dm,
                R.id.q9am, R.id.q9bm, R.id.q9cm, R.id.q9dm,
                R.id.q10am, R.id.q10bm, R.id.q10cm, R.id.q10dm
        };
        for (int id : buttonIds) {
            Espresso.onView(withId(id)).perform(ViewActions.scrollTo(), ViewActions.click());
        }
        Espresso.onView(withId(R.id.submath)).perform(ViewActions.scrollTo(), ViewActions.click());
        Espresso.onView(withId(R.id.result_show))
                .check(ViewAssertions.matches(withText("Your Score: 0")));
    }
 @Test
    public void noAnswerSubmit() {
     Espresso.onView(withId(R.id.submath)).perform(ViewActions.scrollTo(), ViewActions.click());
     Espresso.onView(withId(R.id.result_show))
             .check(ViewAssertions.matches(withText("Your Score: 0")));

 }
 @Test
    public void ClickingWrongAnswerThenCorrectAnswer() {
     Espresso.onView(withId(R.id.q1am)).perform(ViewActions.scrollTo(), ViewActions.click());
     Espresso.onView(withId(R.id.q1bm)).perform(ViewActions.scrollTo(), ViewActions.click());
     Espresso.onView(withId(R.id.submath)).perform(ViewActions.scrollTo(), ViewActions.click());
     Espresso.onView(withId(R.id.result_show))
             .check(ViewAssertions.matches(withText("Your Score: 0")));
 }
    @Test
    public void ClickingCorrectAnswerThenWrongAnswer() {
        Espresso.onView(withId(R.id.q1bm)).perform(ViewActions.scrollTo(), ViewActions.click());
        Espresso.onView(withId(R.id.q1am)).perform(ViewActions.scrollTo(), ViewActions.click());
        Espresso.onView(withId(R.id.submath)).perform(ViewActions.scrollTo(), ViewActions.click());
        Espresso.onView(withId(R.id.result_show))
                .check(ViewAssertions.matches(withText("Your Score: 1")));
    }

}