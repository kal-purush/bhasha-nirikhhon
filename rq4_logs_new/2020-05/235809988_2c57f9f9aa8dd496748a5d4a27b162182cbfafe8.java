package com.group4sweng.scranplan;

import android.util.Log;

import androidx.test.core.app.ActivityScenario;
import androidx.test.espresso.Espresso;
import androidx.test.espresso.ViewInteraction;
import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.test.rule.ActivityTestRule;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Random;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.action.ViewActions.click;
import static androidx.test.espresso.action.ViewActions.typeText;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;
import static com.group4sweng.scranplan.EspressoHelper.childAtPosition;
import static com.group4sweng.scranplan.EspressoHelper.navigateToRecipe;
import static com.group4sweng.scranplan.EspressoHelper.openSideBar;
import static org.hamcrest.Matchers.allOf;

/**
 *
 * Test the Social Profile.
 * Author(s): LNewman
 * (c) CoDev 2020
 *
 *  Tests are included to make sure user can navigate successfully around the social profile
 *
 * All other tests are manual tests to check data is properly stored,
 * deleted and retrieved within firebase and that the image picker functions work correctly.
 *
 * Manual Tests include:
 *  - Finding another user and following them
 *  - Ensuring that private users have their profile properties hidden to other users
 *  - Ensuring a public user could be instantly followed
 *  - Ensuring a private users could be requested to followed
 *  - Ensuring notifications populated correctly
 *  - Ensuring notifications could be accepted and deleted with the correct outcomes
 *  - Ensuring that a followed private users could be viewed with the correct privacy
 *  - Ensuring all navigation to profile is correct
 *  - Ensure privacy settings alterations for enable changes operate as they should
 *
 *  Other tests:
 *  - upgrade to login screen functional
 *  - Unique usernames working correctly and populate with visual indications
 *  - Complex passwords operational with visual indications
 *  - Usernames also changed in profile settings
 *
 *  -- USER STORY TESTS LINKED WITH ---
 *  C20,A1
 */
@RunWith(AndroidJUnit4.class)
public class SocialProfileTest implements Credentials  {

    //  Android Log tag.
    String TAG = "socialProfileTest";
    String test;


    //  How long we should sleep when waiting for Firebase information to update. Increase this value if you have a slower machine or emulator.
    private static final int THREAD_SLEEP_TIME = 4000;

    @Rule
    public ActivityTestRule<Home> mActivityTestRule = new ActivityTestRule<Home>(Home.class);

    //  Login with the associated test credentials before testing, wait for Firebase to update and enter feed fragment.
    @Before
    public void setUp() throws InterruptedException {

        Log.i(TAG, "Starting tests");
        Random random = new Random();
        test = "test" + random.nextInt();

        ActivityScenario.launch(Login.class);//Launch the login screen
        Thread.sleep(THREAD_SLEEP_TIME);


        onView(withId(R.id.loginButton))
                .perform(click());

        onView(withId(R.id.emailEditText))
                .perform(typeText(TEST_EMAIL));
        onView(withId(R.id.passwordEditText))
                .perform(typeText(TEST_PASSWORD));
        Espresso.closeSoftKeyboard();

        onView(withId(R.id.loginButton))
                .perform(click());

        Thread.sleep(THREAD_SLEEP_TIME);

    }






    // Check all fields populate in profile
    @Test
    public void checkProfile() throws InterruptedException {
        openSideBar(EspressoHelper.SideBarElement.PROFILE);
        Thread.sleep(THREAD_SLEEP_TIME);

        ViewInteraction tabView2 = onView(
                allOf(childAtPosition(
                        childAtPosition(
                                withId(R.id.profileStreamTabs),
                                0),
                        1),
                        isDisplayed()));
        tabView2.perform(click());

        Thread.sleep(THREAD_SLEEP_TIME/4);


        ViewInteraction tabView3 = onView(
                allOf(childAtPosition(
                        childAtPosition(
                                withId(R.id.profileStreamTabs),
                                0),
                        2),
                        isDisplayed()));
        tabView3.perform(click());


    }



    // Check all navigation to notifications
    @Test
    public void checkNotifications() throws InterruptedException {
        openSideBar(EspressoHelper.SideBarElement.NOTIFICATION);
        Thread.sleep(THREAD_SLEEP_TIME);


    }



    @After
    public void finishOff() {
    }
}