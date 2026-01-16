package com.example.study;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.action.ViewActions.click;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;

import androidx.test.ext.junit.rules.ActivityScenarioRule;
import androidx.test.ext.junit.runners.AndroidJUnit4;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(AndroidJUnit4.class)
public class BiologyActivityTest {

    @Rule
    public ActivityScenarioRule<BiologyActivity> activityRule =
            new ActivityScenarioRule<>(BiologyActivity.class);

    @Test
    public void testButtonClick() {
        // Find the button with the ID "my_button" and click it
        onView(withId(R.id.next_button)).perform(click());
    }
}