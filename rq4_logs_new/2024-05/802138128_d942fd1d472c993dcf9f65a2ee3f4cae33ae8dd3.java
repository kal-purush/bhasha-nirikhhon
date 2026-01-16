package com.example.study;

import androidx.test.core.app.ActivityScenario;
import androidx.test.espresso.Espresso;
import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.matcher.ViewMatchers;
import androidx.test.ext.junit.rules.ActivityScenarioRule;
import org.junit.Rule;
import org.junit.Test;

import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.*;
import static org.hamcrest.Matchers.not;

public class LoginActivityUITest {

    @Rule
    public ActivityScenarioRule<LoginActivity> activityScenarioRule = new ActivityScenarioRule<>(LoginActivity.class);

    @Test
    public void testLoginButton_Click_ProgressBarVisible() {
        ActivityScenario<LoginActivity> scenario = activityScenarioRule.getScenario();
        Espresso.onView(ViewMatchers.withId(R.id.logbtn)).perform(ViewActions.click());
        Espresso.onView(ViewMatchers.withId(R.id.logprog)).check(matches(isDisplayed()));
    }

    @Test
    public void testRegistrationButton_Click_OpenRegisterActivity() {
        ActivityScenario<LoginActivity> scenario = activityScenarioRule.getScenario();
        Espresso.onView(ViewMatchers.withId(R.id.logreg)).perform(ViewActions.click());
        // Add assertions to verify RegisterActivity starts
    }

    @Test
    public void testSuccessfulLogin() {
        ActivityScenario<LoginActivity> scenario = activityScenarioRule.getScenario();
        // Enter valid email and password into the respective EditText fields
        Espresso.onView(ViewMatchers.withId(R.id.logmail)).perform(ViewActions.typeText("validemail@example.com"));
        Espresso.onView(ViewMatchers.withId(R.id.logpass)).perform(ViewActions.typeText("password"));
        Espresso.onView(ViewMatchers.withId(R.id.logbtn)).perform(ViewActions.click());
        // Add assertions to verify successful login
    }

    @Test
    public void testEmptyFields_Login_NoAction() {
        ActivityScenario<LoginActivity> scenario = activityScenarioRule.getScenario();
        // Leave both email and password fields empty
        Espresso.onView(ViewMatchers.withId(R.id.logbtn)).perform(ViewActions.click());
        // Add assertions to verify no action is taken
    }

    @Test
    public void testProgressBar_Visibility_DuringLoginProcess() {
        ActivityScenario<LoginActivity> scenario = activityScenarioRule.getScenario();
        // Enter valid email and password into the respective EditText fields
        Espresso.onView(ViewMatchers.withId(R.id.logmail)).perform(ViewActions.typeText("validemail@example.com"));
        Espresso.onView(ViewMatchers.withId(R.id.logpass)).perform(ViewActions.typeText("password"));
        Espresso.onView(ViewMatchers.withId(R.id.logbtn)).perform(ViewActions.click());
        Espresso.onView(ViewMatchers.withId(R.id.logprog)).check(matches(isDisplayed()));
    }

    @Test
    public void testInvalidEmail_ValidPassword() {
        ActivityScenario<LoginActivity> scenario = activityScenarioRule.getScenario();

        // Enter invalid email and valid password into the respective EditText fields
        Espresso.onView(ViewMatchers.withId(R.id.logmail)).perform(ViewActions.clearText(), ViewActions.typeText("invalidemail")); // Invalid email format
        Espresso.onView(ViewMatchers.withId(R.id.logpass)).perform(ViewActions.clearText(), ViewActions.typeText("password"));

        // Click on the login button
        Espresso.onView(ViewMatchers.withId(R.id.logbtn)).perform(ViewActions.click());

        // Ensure that the progress bar is not displayed
        Espresso.onView(ViewMatchers.withId(R.id.logprog)).check(matches(not(isDisplayed())));
    }
}