package com.example.study;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;

import static java.util.regex.Pattern.matches;

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
public class

ChemistryActivityUITest {

    @Rule
    public ActivityScenarioRule<ChemistryActivity> activityScenarioRule = new ActivityScenarioRule<>(ChemistryActivity.class);

    @Test
    public void testQuizFlow() {
        // Verify the initial question
        onView(withId(R.id.question_text))
                .check(ViewAssertions.matches(withText("What is the symbol for sodium?")));

        // Enter correct answer and submit
        onView(withId(R.id.answer_edit_text))
                .perform(ViewActions.typeText("Na"), ViewActions.closeSoftKeyboard());
        onView(withId(R.id.submit_button)).perform(ViewActions.click());

         //Verify the next question
        onView(withId(R.id.question_text))
                .check(ViewAssertions.matches(withText("What is the basic unit of mass in the SI system?")));

        // Enter incorrect answer and submit
        onView(withId(R.id.answer_edit_text))
                .perform(ViewActions.typeText("kilogram"), ViewActions.closeSoftKeyboard());
        onView(withId(R.id.submit_button)).perform(ViewActions.click());

        // Verify the next question
        onView(withId(R.id.question_text))
                .check(ViewAssertions.matches(withText("What is the process of converting a gas into a liquid?")));

        // Continue testing the rest of the quiz flow in a similar manner
    }

    @Test
    public void testInitialQuestionDisplayed() {
        // Check if the first question is displayed correctly
        onView(withId(R.id.question_text))
                .check(matches(withText("What is the symbol for sodium?")));
    }

    @Test
    public void testSecondQuestionDisplayed() {
        // Check if the second question is displayed correctly after answering the first question
        onView(withId(R.id.answer_edit_text)).perform(ViewActions.typeText("Na"), ViewActions.closeSoftKeyboard());
        onView(withId(R.id.submit_button)).perform(ViewActions.click());

        onView(withId(R.id.question_text))
                .check(matches(withText("What is the basic unit of mass in the SI system?")));
    }

    @Test
    public void testThirdQuestionDisplayed() {
        // Check if the third question is displayed correctly after answering the first two questions
        onView(withId(R.id.answer_edit_text)).perform(ViewActions.typeText("Na"), ViewActions.closeSoftKeyboard());
        onView(withId(R.id.submit_button)).perform(ViewActions.click());

        onView(withId(R.id.answer_edit_text)).perform(ViewActions.typeText("gram"), ViewActions.closeSoftKeyboard());
        onView(withId(R.id.submit_button)).perform(ViewActions.click());

        onView(withId(R.id.question_text))
                .check(matches(withText("What is the process of converting a gas into a liquid?")));
    }

    // Similarly, you can add tests for the fourth and fifth questions if needed.
}

//}