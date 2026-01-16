package com.ui.espresso.view;

import androidx.test.espresso.Espresso;
import androidx.test.ext.junit.rules.ActivityScenarioRule;
import androidx.test.ext.junit.runners.AndroidJUnit4;

import com.ui.espresso.R;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.action.ViewActions.click;
import static androidx.test.espresso.action.ViewActions.typeText;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;

@RunWith(AndroidJUnit4.class)
public class DashboardTest {

    @Rule
    public ActivityScenarioRule<Dashboard> activityRule =
            new ActivityScenarioRule<>(Dashboard.class);

    private static String DASHBOARD = "This is Dashboard";
    private static String MESSAGE = "Welcome!";
    private static String NICKNAME = "Akira";

    @Test
    public void testWelcomeMsg() {
        // input some text in the edit text
        onView(withId(R.id.dash))
                .perform(typeText(DASHBOARD));

        onView(withId(R.id.msg))
                .perform(typeText(MESSAGE));

        onView(withId(R.id.name))
                .perform(typeText(NICKNAME));

        // close soft keyboard
        Espresso.closeSoftKeyboard();

        // perform button click
        onView(withId(R.id.btn))
                .perform(click());

        // checking the text in the textview
        onView(withId(R.id.tv))
                .check(matches(withText(DASHBOARD +" "+ MESSAGE +" "+ NICKNAME)));
    }

}