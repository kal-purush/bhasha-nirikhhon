package com.example.study;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.action.ViewActions.closeSoftKeyboard;
import static androidx.test.espresso.action.ViewActions.typeText;

import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.assertion.ViewAssertions;
import androidx.test.espresso.matcher.ViewMatchers;
import androidx.test.ext.junit.rules.ActivityScenarioRule;

import org.junit.Rule;
import org.junit.Test;

public class InfoActivityUITest {
    @Rule
    public ActivityScenarioRule<InfoActivity> activityActivityScenarioRule = new ActivityScenarioRule<InfoActivity>(InfoActivity.class);

    @Test
    public void test1()
    {

        onView(ViewMatchers.withId(R.id.infobtn)).perform(ViewActions.click());
        onView(ViewMatchers.withId(R.id.infotv)).check(ViewAssertions.matches(ViewMatchers.withText("Nafis")));
        onView(ViewMatchers.withId(R.id.infobtn)).perform(ViewActions.click());
        onView(ViewMatchers.withId(R.id.infotv)).check(ViewAssertions.matches(ViewMatchers.withText("Hafiz")));
        onView(ViewMatchers.withId(R.id.infobtn)).perform(ViewActions.click());
        onView(ViewMatchers.withId(R.id.infotv)).check(ViewAssertions.matches(ViewMatchers.withText("Tomal")));

    }

}