/*
package com.example.its_a_feature_not_a_bug;

import static androidx.test.espresso.Espresso.onData;
import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.action.ViewActions.click;
import static androidx.test.espresso.assertion.ViewAssertions.doesNotExist;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;

import static org.hamcrest.CoreMatchers.anything;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;

import androidx.test.core.app.ActivityScenario;
import androidx.test.espresso.action.ViewActions;
import androidx.test.ext.junit.rules.ActivityScenarioRule;
import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.test.filters.LargeTest;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(AndroidJUnit4.class)
@LargeTest
public class MainActivityTest{
    @Rule
    public ActivityScenarioRule<MainActivity> scenario = new ActivityScenarioRule<MainActivity>(MainActivity.class);
    @Test
    public void testAddEvent(){
        // Click on Add Event button
        onView(withId(R.id.fab_add_event)).perform(click());
        // Type "STUDYING" in the editText
        onView(withId(R.id.edit_text_event_title)).perform(ViewActions.typeText("STUDYING"));
                // Click on builder ok button set positive button
                onView(withId(R.id.)).perform(click());

        onView(withText("STUDYING")).check(matches(isDisplayed()));
    }
    @Test
    public void testListView(){
    // Add a event
        onView(withId(R.id.fab_add_event)).perform(click());
        onView(withId(R.id.edit_text_event_title)).perform(ViewActions.typeText("STUDYING"));
        // check if the event is displayed
        onView(withText("STUDYING")).check(matches(isDisplayed()));

    }
    @Test
    public void testBackButton(){
        // Add a event
        onView(withId(R.id.fab_add_event)).perform(click());
        // Enter event name
        onView(withId(R.id.edit_text_event_title)).perform(ViewActions.typeText("STUDYING"));
        onView(withId(R.id.)).perform(click());
        // Click on the added event

        // Click on the back button
        onView(withId(R.id.)).perform(click());
        // Check if the MainActivity is displayed
        onView(withText("STUDYING")).check(doesNotExist());
    }
    @Test
    public void testActivitySwitch(){
        // Add a event
        onView(withId(R.id.fab_add_event)).perform(click());
        // Enter event name
        onView(withId(R.id.edit_text_event_title)).perform(ViewActions.typeText("STUDYING"));
        // Click on Confirm
        onView(withId(R.id.)).perform(click());
        // Click on the event name
        onData(anything()).inAdapterView(withId(R.id.)).atPosition(0).perform(click());
        // Check if the activity switched
        onView(withText("STUDYING")).check(matches(isDisplayed()));
    }
}
*/

