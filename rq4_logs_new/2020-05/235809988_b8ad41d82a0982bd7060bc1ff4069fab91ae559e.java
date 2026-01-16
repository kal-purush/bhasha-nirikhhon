package com.group4sweng.scranplan.MealPlanner;

import android.util.Log;
import android.view.KeyEvent;
import android.widget.SearchView;

import androidx.test.core.app.ActivityScenario;
import androidx.test.espresso.Espresso;
import androidx.test.espresso.matcher.ViewMatchers;
import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.test.rule.ActivityTestRule;

import com.group4sweng.scranplan.Credentials;
import com.group4sweng.scranplan.Home;
import com.group4sweng.scranplan.Login;
import com.group4sweng.scranplan.R;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.action.ViewActions.click;
import static androidx.test.espresso.action.ViewActions.longClick;
import static androidx.test.espresso.action.ViewActions.pressBack;
import static androidx.test.espresso.action.ViewActions.pressKey;
import static androidx.test.espresso.action.ViewActions.typeText;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.isAssignableFrom;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.isRoot;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;
import static com.group4sweng.scranplan.HomeTest.typeSearchViewText;


public class ShoppingListTest implements Credentials {

    @Rule
    public ActivityTestRule<Home> mActivityTestRule = new ActivityTestRule<>(Home.class);

    private String TAG = "ShoppingListTest";

    private static final int THREAD_SLEEP_TIME = 4000;

    //Logging in with test account
    @Before
    public void setUp() throws Exception {

        Log.d(TAG, "Starting tests");
        ActivityScenario.launch(Login.class);

        onView(ViewMatchers.withId(R.id.loginButton)).perform(click());
        onView(withId(R.id.emailEditText)).perform(typeText(TEST_EMAIL));
        onView(withId(R.id.passwordEditText)).perform(typeText(TEST_PASSWORD));
        Espresso.closeSoftKeyboard();

        onView(withId(R.id.loginButton)).perform(click());
        Thread.sleep(THREAD_SLEEP_TIME);
    }

    //Checks correct elements appear when add button is pressed
    @Test
    public void searchAndAddToPlanner() throws InterruptedException {

        Log.d(TAG, "Testing searching");

        Log.d(TAG, "Testing searching");

        onView(withText("Meal Planner")).perform(click());
        onView(withId(0)).perform(click());
        onView(withId(R.id.menuSearch)).perform(click());
        onView(isAssignableFrom(SearchView.class))
                .perform(typeSearchViewText("bacon"))
                .perform(pressKey(KeyEvent.KEYCODE_ENTER));

        Thread.sleep(THREAD_SLEEP_TIME/4);

        onView(withText("Bacon Sandwich")).perform(click());
        onView(withText("Add")).perform(click());

    }

    @After
    public void tearDown() { Log.d(TAG, "Tests complete"); }
}