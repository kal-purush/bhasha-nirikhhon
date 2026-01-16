package com.group4sweng.scranplan;

import android.util.Log;
import android.view.KeyEvent;
import android.view.View;
import android.widget.SearchView;

import androidx.test.core.app.ActivityScenario;
import androidx.test.espresso.Espresso;
import androidx.test.espresso.UiController;
import androidx.test.espresso.ViewAction;
import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.test.filters.LargeTest;
import androidx.test.rule.ActivityTestRule;

import com.group4sweng.scranplan.UserInfo.UserInfoPrivate;

import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.action.ViewActions.click;
import static androidx.test.espresso.action.ViewActions.pressKey;
import static androidx.test.espresso.action.ViewActions.typeText;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.isAssignableFrom;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;
import static org.hamcrest.Matchers.allOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 *
 * Test the Feed Fragment.
 * Author(s): LNewman
 * (c) CoDev 2020
 *
 *  Tests are included to make sure information is displayed, user can successfully after search
 *  parameters and search for the meals they want.
 */
@RunWith(AndroidJUnit4.class)
@LargeTest
public class FeedTest {

    //  Android Log tag.
    String TAG = "homeTest";

    private UserInfoPrivate testUser;

    //  Default test values
    private static final String TEST_EMAIL = "lifn@york.ac.uk";
    private static String TEST_PASSWORD = "password";

    //  How long we should sleep when waiting for Firebase information to update. Increase this value if you have a slower machine or emulator.
    private static final int THREAD_SLEEP_TIME = 4000;

    @Rule
    public ActivityTestRule<Home> mActivityTestRule = new ActivityTestRule<Home>(Home.class);

    //  Login with the associated test credentials before testing, wait for Firebase to update and enter profile settings.
    @Before
    public void setUp() throws InterruptedException {

        Log.i(TAG, "Starting tests");

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

    }

    // Check search field can be activated and searched in, also testing results.
    @Test
    public void testSearchForBacon() throws InterruptedException {
        onView(withId(R.id.menuSearch)).perform(click());
        onView(isAssignableFrom(SearchView.class))
                .perform(typeSearchViewText("bacon"))
                .perform(pressKey(KeyEvent.KEYCODE_ENTER));


        Thread.sleep(THREAD_SLEEP_TIME/4);

        onView(withText("Bacon Sandwich"))
                .check(matches(isDisplayed()));

        Espresso.pressBack();

    }

    // Insert text to search bar
    public static ViewAction typeSearchViewText(final String text){
        return new ViewAction(){
            @Override
            public Matcher<View> getConstraints() {
                //Ensures that only applied when search view is visible
                return allOf(isDisplayed(), isAssignableFrom(SearchView.class));
            }

            @Override
            public String getDescription() {
                return "Change view text";
            }

            @Override
            public void perform(UiController uiController, View view) {
                ((SearchView) view).setQuery(text,false);
            }


        };
    }



        /* Checks all filters operate and are saved */
    @Test
    public void testFilterAndPrivacyInfoIsStoredAndRetrieved() throws InterruptedException {
        HashMap<String, Boolean> initialSettings = new HashMap<>();


        //  Store all initial filters in a HashMap.
        initialSettings.put("pescatarian",  mActivityTestRule.getActivity().mPescatarianBox.isChecked());
        initialSettings.put("vegetarian",  mActivityTestRule.getActivity().mVegetarianBox.isChecked());
        initialSettings.put("vegan",  mActivityTestRule.getActivity().mVeganBox.isChecked());

        initialSettings.put("eggs",  mActivityTestRule.getActivity().mEggsBox.isChecked());
        initialSettings.put("milk",  mActivityTestRule.getActivity().mMilkBox.isChecked());
        initialSettings.put("soya",  mActivityTestRule.getActivity().mSoyBox.isChecked());
        initialSettings.put("gluten",  mActivityTestRule.getActivity().mWheatBox.isChecked());
        initialSettings.put("shellfish",  mActivityTestRule.getActivity().mShellfishBox.isChecked());
        initialSettings.put("nuts",  mActivityTestRule.getActivity().mNutsBox.isChecked());

        initialSettings.put("score",  mActivityTestRule.getActivity().mScoreBox.isChecked());
        initialSettings.put("vote",  mActivityTestRule.getActivity().mVoteBox.isChecked());
        initialSettings.put("time",  mActivityTestRule.getActivity().mTimeBox.isChecked());

        initialSettings.put("ingred",  mActivityTestRule.getActivity().mIngredientsBox.isChecked());
        initialSettings.put("name",  mActivityTestRule.getActivity().mNameBox.isChecked());
        initialSettings.put("chef",  mActivityTestRule.getActivity().mChefBox.isChecked());

        // Open up filter menu
        onView(withId(R.id.menuSortButton)).perform(click());

        //  Change every switch and Checkboxes value.
        onView(withId(R.id.chefCheckBox))
                .perform(click());

        // Change tab
        onView(withText("Diet")).perform(click());

        onView(withId(R.id.menuPescatarianCheckBox))
                .perform(click());
        onView(withId(R.id.menuNutCheckBox))
                .perform(click());
        onView(withId(R.id.menuEggCheckBox))
                .perform(click());
        onView(withId(R.id.menuMilkCheckBox))
                .perform(click());
        onView(withId(R.id.menuWheatCheckBox))
                .perform(click());
        onView(withId(R.id.menuSoyCheckBox))
                .perform(click());

        // Change tab
        onView(withText("Sort")).perform(click());

        onView(withId(R.id.voteCheckBox))
                .perform(click());

        // Close filter box
        onView(withText("OK")).perform(click());


        Thread.sleep(THREAD_SLEEP_TIME/4);

        //  Store all initial filters in a HashMap.
        assertNotEquals(initialSettings.get("pescatarian"),  mActivityTestRule.getActivity().mPescatarianBox.isChecked());
        assertEquals(initialSettings.get("vegetarian"),  mActivityTestRule.getActivity().mVegetarianBox.isChecked());
        assertEquals(initialSettings.get("vegan"),  mActivityTestRule.getActivity().mVeganBox.isChecked());

        assertNotEquals(initialSettings.get("eggs"),  mActivityTestRule.getActivity().mEggsBox.isChecked());
        assertNotEquals(initialSettings.get("milk"),  mActivityTestRule.getActivity().mMilkBox.isChecked());
        assertNotEquals(initialSettings.get("soya"),  mActivityTestRule.getActivity().mSoyBox.isChecked());
        assertNotEquals(initialSettings.get("gluten"),  mActivityTestRule.getActivity().mWheatBox.isChecked());
        assertEquals(initialSettings.get("shellfish"),  mActivityTestRule.getActivity().mShellfishBox.isChecked());
        assertNotEquals(initialSettings.get("nuts"),  mActivityTestRule.getActivity().mNutsBox.isChecked());

        assertNotEquals(initialSettings.get("score"),  mActivityTestRule.getActivity().mScoreBox.isChecked());
        assertNotEquals(initialSettings.get("vote"),  mActivityTestRule.getActivity().mVoteBox.isChecked());
        assertEquals(initialSettings.get("time"),  mActivityTestRule.getActivity().mTimeBox.isChecked());

        assertNotEquals(initialSettings.get("ingred"),  mActivityTestRule.getActivity().mIngredientsBox.isChecked());
        assertEquals(initialSettings.get("name"),  mActivityTestRule.getActivity().mNameBox.isChecked());
        assertNotEquals(initialSettings.get("chef"),  mActivityTestRule.getActivity().mChefBox.isChecked());

    }


    // Check search vegetarian search for bacon returns nothing.
    @Test
    public void testSearchForVegetarianBacon() throws InterruptedException {
        // Open up filter menu
        onView(withId(R.id.menuSortButton)).perform(click());


        // Change tab
        onView(withText("Diet")).perform(click());

        onView(withId(R.id.menuVegCheckBox))
                .perform(click());

        // Change tab
        onView(withText("Sort")).perform(click());

        onView(withId(R.id.voteCheckBox))
                .perform(click());

        // Close filter box
        onView(withText("OK")).perform(click());


        Thread.sleep(THREAD_SLEEP_TIME/4);


        onView(withId(R.id.menuSearch)).perform(click());
        onView(isAssignableFrom(SearchView.class))
                .perform(typeSearchViewText("bacon"))
                .perform(pressKey(KeyEvent.KEYCODE_ENTER));


        Thread.sleep(THREAD_SLEEP_TIME/4);

        onView(withText("No more results"))
                .check(matches(isDisplayed()));

        Espresso.pressBack();

    }

    // Check search Gluten allergy search for bacon returns nothing.
    @Test
    public void testSearchForGlutenFreeBacon() throws InterruptedException {
        // Open up filter menu
        onView(withId(R.id.menuSortButton)).perform(click());


        // Change tab
        onView(withText("Diet")).perform(click());

        onView(withId(R.id.menuWheatCheckBox))
                .perform(click());

        // Change tab
        onView(withText("Sort")).perform(click());

        onView(withId(R.id.voteCheckBox))
                .perform(click());

        // Close filter box
        onView(withText("OK")).perform(click());


        Thread.sleep(THREAD_SLEEP_TIME/4);


        onView(withId(R.id.menuSearch)).perform(click());
        onView(isAssignableFrom(SearchView.class))
                .perform(typeSearchViewText("bacon"))
                .perform(pressKey(KeyEvent.KEYCODE_ENTER));


        Thread.sleep(THREAD_SLEEP_TIME/4);

        onView(withText("No more results"))
                .check(matches(isDisplayed()));

        Espresso.pressBack();

    }



    @After
    public void finishOff() {
    }
}