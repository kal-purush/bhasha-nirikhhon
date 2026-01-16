package com.example.study;

import androidx.test.core.app.ActivityScenario;
import androidx.test.espresso.Espresso;
import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.matcher.ViewMatchers;
import androidx.test.ext.junit.rules.ActivityScenarioRule;
import org.junit.Rule;
import org.junit.Test;

public class HomepageUITest {

    @Rule
    public ActivityScenarioRule<Homepage> activityRule =
            new ActivityScenarioRule<>(Homepage.class);

    @Test
    public void testQuizButtonClick() {
        Espresso.onView(ViewMatchers.withId(R.id.quize)).perform(ViewActions.click());
    }

    @Test
    public void testGradeButtonClick() {
        Espresso.onView(ViewMatchers.withId(R.id.grade)).perform(ViewActions.click());
    }

    @Test
    public void testInfoButtonClick() {
        Espresso.onView(ViewMatchers.withId(R.id.info)).perform(ViewActions.click());
    }

    @Test
    public void testHotlineButtonClick() {
        Espresso.onView(ViewMatchers.withId(R.id.call)).perform(ViewActions.click());
    }

    @Test
    public void testBackNavigationFromMainActivity() {
        Espresso.onView(ViewMatchers.withId(R.id.quize)).perform(ViewActions.click());
        Espresso.pressBack();
    }

    @Test
    public void testBackNavigationFromGradeActivity() {
        Espresso.onView(ViewMatchers.withId(R.id.grade)).perform(ViewActions.click());
        Espresso.pressBack();
    }
}