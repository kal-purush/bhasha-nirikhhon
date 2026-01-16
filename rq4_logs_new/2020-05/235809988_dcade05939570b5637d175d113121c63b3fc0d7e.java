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
 * Test the Feed Fragment.
 * Author(s): LNewman
 * (c) CoDev 2020
 *
 *  Tests are included to make sure user can navigate, view all correct posts, view posts, move
 *  around the profile, having all posts and comments load correctly, in the correct places
 *  and in order. Finally it was checked if a user can post and attach all parts and delete each
 *  part.
 *
 * All other tests are manual tests to check data is properly stored,
 * deleted and retrieved within firebase and that the image picker functions work correctly.
 *
 * Manual Tests include:
 *  - Image picker works as expected with correct accepted formats & filesize only.
 *  - recipes can be added and load in correctly -> also saved correctly
 *  - All posts can be viewed, followers is updated to only show 3 posts
 *  - Three sections of profile show correct posts and all infinite scrolls function
 *  - All parts can be added to posts
 *  - All likes function and save correctly, only one person can like any one post and this like
 *  is saved
 *  - Reviews can be added to posts in the same way they are added to a particular recipe
 *
 *  -- USER STORY TESTS LINKED WITH ---
 *  C20,A1
 */
@RunWith(AndroidJUnit4.class)
public class SocialTest implements Credentials  {

    //  Android Log tag.
    String TAG = "FeedTest";
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

        ActivityScenario.launch(Login.class); //Launch the login screen


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

        onView(withText("FEED"))
                .perform(click());

    }



    // Test adding a recipe and a rating to a post
    @Test
    public void testRecipeAndRating() throws InterruptedException {
        Log.d(TAG, "Testing navigate to add recipe");


        onView(withId(R.id.recipeIcon))
                .perform(click());
        Thread.sleep(THREAD_SLEEP_TIME/4);
        onView(withId(R.id.menuSearch)).check(
                matches(isDisplayed()));
        navigateToRecipe("Braised peas with bacon, lentils and cod");
        Thread.sleep(THREAD_SLEEP_TIME);
        onView(withText("Add")).perform(click());


        Thread.sleep(THREAD_SLEEP_TIME);
        onView(withId(R.id.reviewIcon))
                .perform(click());

        onView(withId(R.id.postRecipeRating)).check(matches(isDisplayed()));

    }

//     Posts can be sent from feed
    @Test
    public void testPost() throws InterruptedException {
        Log.d(TAG, "Testing navigate to add recipe");

        onView(withId(R.id.postBodyInput))
                .perform(typeText(test));
        Espresso.closeSoftKeyboard();

        Thread.sleep(THREAD_SLEEP_TIME/4);

        onView(withId(R.id.sendPostButton))
                .perform(click());

        Thread.sleep(THREAD_SLEEP_TIME);


        onView(withText(test))
                .perform(click());
        Thread.sleep(THREAD_SLEEP_TIME);

        onView(withId(R.id.postMenu))
                .perform(click());


        Thread.sleep(THREAD_SLEEP_TIME);


    }

//    Check that posts can be posed and displayed correctly
    @Test
    public void checkPosting() throws InterruptedException {
        Log.d(TAG, "Testing navigate to add recipe");

        onView(withId(R.id.postBodyInput))
                .perform(typeText(test));
        Espresso.closeSoftKeyboard();

        Thread.sleep(THREAD_SLEEP_TIME/4);

        onView(withId(R.id.sendPostButton))
                .perform(click());

        Thread.sleep(THREAD_SLEEP_TIME);

        onView(withText(test))
                .check(matches(isDisplayed()));

        openSideBar(EspressoHelper.SideBarElement.PROFILE);
        Thread.sleep(THREAD_SLEEP_TIME);


        onView(withText(test))
                .check(matches(isDisplayed()));


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







    @After
    public void finishOff() {
    }
}