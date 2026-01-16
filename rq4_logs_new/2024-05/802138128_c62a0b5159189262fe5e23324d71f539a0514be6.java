package com.example.study;

import androidx.test.espresso.Espresso;
import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.matcher.ViewMatchers;
import androidx.test.ext.junit.rules.ActivityScenarioRule;
import androidx.test.filters.LargeTest;

import org.junit.Rule;
import org.junit.Test;

import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;

@LargeTest
public class GradeUITest {

    @Rule
    public ActivityScenarioRule<Grade> activityScenarioRule = new ActivityScenarioRule<>(Grade.class);

    @Test
    public void calculateGradeAndDisplay() {
        Espresso.onView(ViewMatchers.withId(R.id.gradedt))
                .perform(ViewActions.typeText("85"), ViewActions.closeSoftKeyboard());

        Espresso.onView(ViewMatchers.withId(R.id.gradebtn)).perform(ViewActions.click());

        Espresso.onView(withId(R.id.grade_output))
                .check(matches(isDisplayed()))
                .check(matches(withText("Congratulations !!! You have got A+")));
    }

    @Test
    public void gradeA() {
        Espresso.onView(withId(R.id.gradedt))
                .perform(ViewActions.typeText("75"), ViewActions.closeSoftKeyboard());

        Espresso.onView(withId(R.id.gradebtn)).perform(ViewActions.click());

        Espresso.onView(withId(R.id.grade_output))
                .check(matches(isDisplayed()))
                .check(matches(withText("Congratulations !!! You have got A")));
    }

    @Test
    public void gradeAMinus() {
        Espresso.onView(withId(R.id.gradedt))
                .perform(ViewActions.typeText("63"), ViewActions.closeSoftKeyboard());

        Espresso.onView(withId(R.id.gradebtn)).perform(ViewActions.click());

        Espresso.onView(withId(R.id.grade_output))
                .check(matches(isDisplayed()))
                .check(matches(withText("Congratulations !!! You have got A-")));
    }

    @Test
    public void gradeF() {
        Espresso.onView(withId(R.id.gradedt))
                .perform(ViewActions.typeText("30"), ViewActions.closeSoftKeyboard());

        Espresso.onView(withId(R.id.gradebtn)).perform(ViewActions.click());

        Espresso.onView(withId(R.id.grade_output))
                .check(matches(isDisplayed()))
                .check(matches(withText("Sorry Your grade is F")));
    }


    @Test
    public void invalidMark() {
        Espresso.onView(withId(R.id.gradedt))
                .perform(ViewActions.typeText("110"), ViewActions.closeSoftKeyboard());

        Espresso.onView(withId(R.id.gradebtn)).perform(ViewActions.click());

        Espresso.onView(withId(R.id.grade_output))
                .check(matches(isDisplayed()))
                .check(matches(withText("Please Enter Valid Mark")));
    }
}