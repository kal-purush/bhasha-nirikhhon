package com.example.study;

import androidx.test.ext.junit.rules.ActivityScenarioRule;
import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.test.espresso.Espresso;
import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.assertion.ViewAssertions;
import androidx.test.espresso.matcher.ViewMatchers;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(AndroidJUnit4.class)
public class PhysicsActivityTest {

    @Rule
    public ActivityScenarioRule<PhysicsActivity> activityActivityScenarioRule = new ActivityScenarioRule<PhysicsActivity>(PhysicsActivity.class);


    @Rule
    public ActivityScenarioRule<PhysicsActivity> activityRule =
            new ActivityScenarioRule<>(PhysicsActivity.class);

    @Test
    public void testCorrectButtonClicked() {
        // Click on the correct button for question 1
        Espresso.onView(ViewMatchers.withId(R.id.q1cp)).perform(ViewActions.click());




        // Check if the other buttons for question 1 are disabled
        Espresso.onView(ViewMatchers.withId(R.id.q1ap))
                .check(ViewAssertions.matches(ViewMatchers.isNotEnabled()));
        Espresso.onView(ViewMatchers.withId(R.id.q1bp))
                .check(ViewAssertions.matches(ViewMatchers.isNotEnabled()));
        Espresso.onView(ViewMatchers.withId(R.id.q1dp))
                .check(ViewAssertions.matches(ViewMatchers.isNotEnabled()));
    }

    @Test
    public void testIncorrectButtonClicked() {
        // Click on the incorrect button for question 1
        Espresso.onView(ViewMatchers.withId(R.id.q1ap)).perform(ViewActions.click());


        // Check if the other buttons for question 1 are disabled
        Espresso.onView(ViewMatchers.withId(R.id.q1bp))
                .check(ViewAssertions.matches(ViewMatchers.isNotEnabled()));
        Espresso.onView(ViewMatchers.withId(R.id.q1cp))
                .check(ViewAssertions.matches(ViewMatchers.isNotEnabled()));
        Espresso.onView(ViewMatchers.withId(R.id.q1dp))
                .check(ViewAssertions.matches(ViewMatchers.isNotEnabled()));
    }


    @Test
    public void testIncorrectButtonClicked2() {
        // Click on the incorrect button for question 1
        Espresso.onView(ViewMatchers.withId(R.id.q1bp)).perform(ViewActions.click());


        // Check if the other buttons for question 1 are disabled
        Espresso.onView(ViewMatchers.withId(R.id.q1ap))
                .check(ViewAssertions.matches(ViewMatchers.isNotEnabled()));
        Espresso.onView(ViewMatchers.withId(R.id.q1cp))
                .check(ViewAssertions.matches(ViewMatchers.isNotEnabled()));
        Espresso.onView(ViewMatchers.withId(R.id.q1dp))
                .check(ViewAssertions.matches(ViewMatchers.isNotEnabled()));
    }

    @Test
    public void testIncorrectButtonClicked3() {
        // Click on the incorrect button for question 1
        Espresso.onView(ViewMatchers.withId(R.id.q1dp)).perform(ViewActions.click());


        // Check if the other buttons for question 1 are disabled
        Espresso.onView(ViewMatchers.withId(R.id.q1ap))
                .check(ViewAssertions.matches(ViewMatchers.isNotEnabled()));
        Espresso.onView(ViewMatchers.withId(R.id.q1bp))
                .check(ViewAssertions.matches(ViewMatchers.isNotEnabled()));
        Espresso.onView(ViewMatchers.withId(R.id.q1cp))
                .check(ViewAssertions.matches(ViewMatchers.isNotEnabled()));
    }
}
