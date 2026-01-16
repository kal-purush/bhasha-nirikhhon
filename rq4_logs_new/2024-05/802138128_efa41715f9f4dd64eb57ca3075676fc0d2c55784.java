package com.example.study;

import androidx.test.espresso.intent.Intents;
import androidx.test.espresso.intent.rule.IntentsTestRule;
import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.test.rule.ActivityTestRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.action.ViewActions.closeSoftKeyboard;
import static androidx.test.espresso.action.ViewActions.replaceText;
import static androidx.test.espresso.action.ViewActions.click;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.intent.matcher.IntentMatchers.hasComponent;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;

import com.example.study.R;

@RunWith(AndroidJUnit4.class)
public class RegisterActivityTest {

    @Rule
    public IntentsTestRule<RegisterActivity> activityRule = new IntentsTestRule<>(RegisterActivity.class);

    @Test
    public void testClickToLoginButton() {
        // Perform click action on the login button
        onView(withId(R.id.reglog)).perform(click());

        // Check if the intent to start LoginActivity was fired
        Intents.intended(hasComponent(LoginActivity.class.getName()));
    }

    @Test
    public void testEditTextMailInput() {
        // Define the test email
        String testEmail = "test@example.com";

        // Input the test email into the email EditText
        onView(withId(R.id.regmail)).perform(replaceText(testEmail), closeSoftKeyboard());

        // Verify that the email EditText contains the inputted email
        onView(withId(R.id.regmail)).check(matches(withText(testEmail)));
    }

    @Test
    public void testEditTextPassInput() {
        // Define the test password
        String testPassword = "password123";

        // Input the test password into the password EditText
        onView(withId(R.id.regpass)).perform(replaceText(testPassword), closeSoftKeyboard());

        // Verify that the password EditText contains the inputted password
        onView(withId(R.id.regpass)).check(matches(withText(testPassword)));
    }

    @Test
    public void testEditTextNameInput() {
        // Define the test name
        String testName = "John Doe";

        // Input the test name into the name EditText
        onView(withId(R.id.editTextText)).perform(replaceText(testName), closeSoftKeyboard());

        // Verify that the name EditText contains the inputted name
        onView(withId(R.id.editTextText)).check(matches(withText(testName)));
    }

    @Test
    public void testEditTextMobileNumberInput() {
        // Define the test mobile number
        String testMobileNumber = "1234567890";

        // Input the test mobile number into the mobile number EditText
        onView(withId(R.id.editTextText2)).perform(replaceText(testMobileNumber), closeSoftKeyboard());

        // Verify that the mobile number EditText contains the inputted mobile number
        onView(withId(R.id.editTextText2)).check(matches(withText(testMobileNumber)));
    }
}