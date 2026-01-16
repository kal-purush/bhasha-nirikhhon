package com.example.study;

import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.test.rule.ActivityTestRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.action.ViewActions.click;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;

@RunWith(AndroidJUnit4.class)
public class InfoActivityTest {

    @Rule
    public ActivityTestRule<InfoActivity> activityRule =
            new ActivityTestRule<>(InfoActivity.class);

    @Test
    public void testButtonClickChangesText() {
        // Click the button and check if the text is "Nafis"
        onView(withId(R.id.infobtn)).perform(click());
        onView(withId(R.id.infotv)).check(matches(withText("Nafis")));

        // Click the button again and check if the text is "Hafiz"
        onView(withId(R.id.infobtn)).perform(click());
        onView(withId(R.id.infotv)).check(matches(withText("Hafiz")));

        // Click the button again and check if the text is "Tomal"
        onView(withId(R.id.infobtn)).perform(click());
        onView(withId(R.id.infotv)).check(matches(withText("Tomal")));

        // Click the button again and check if the text is back to "Nafis"
        onView(withId(R.id.infobtn)).perform(click());
        onView(withId(R.id.infotv)).check(matches(withText("Nafis")));
    }
}