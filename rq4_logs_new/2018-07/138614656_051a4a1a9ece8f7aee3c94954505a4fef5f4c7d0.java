package com.andela.javadevelopers.home.view;

import android.content.Intent;
import android.support.test.espresso.IdlingRegistry;
import android.support.test.espresso.contrib.RecyclerViewActions;
import android.support.test.espresso.intent.rule.IntentsTestRule;
import android.support.test.runner.AndroidJUnit4;

import com.andela.javadevelopers.R;
import com.andela.javadevelopers.userDetail.view.DetailActivity;
import com.andela.javadevelopers.util.EspressoIdlingResource;

import static android.support.test.espresso.Espresso.onView;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static android.support.test.espresso.action.ViewActions.click;
import static android.support.test.espresso.assertion.ViewAssertions.matches;
import static android.support.test.espresso.intent.Intents.intended;
import static android.support.test.espresso.intent.matcher.IntentMatchers.hasComponent;
import static android.support.test.espresso.intent.matcher.IntentMatchers.hasExtraWithKey;
import static android.support.test.espresso.matcher.ViewMatchers.isDisplayed;
import static android.support.test.espresso.matcher.ViewMatchers.withId;
import static org.hamcrest.core.AllOf.allOf;


/**
 * The type Main activity implementation test.
 */
@RunWith(AndroidJUnit4.class)
public class MainActivityImplementationTest {
    /**
     * SCROLL_POSITION.
     */
    private static final int SCROLL_POSITION = 10;

    /**
     * The Rule.
     */
    @Rule
    public IntentsTestRule<MainActivity> rule =
            new IntentsTestRule<>(MainActivity.class, true, false);

    /**
     * Sets up.
     */
    @Before
    public void setUp() {
        rule.launchActivity(new Intent());

        IdlingRegistry.getInstance().register(EspressoIdlingResource.getIdlingResource());
    }

    /**
     * Tear down.
     */
    @After
    public void tearDown() {
        IdlingRegistry.getInstance().unregister(EspressoIdlingResource.getIdlingResource());
    }

    /**
     * Ensure card was clicked.
     */
    @Test
    public void ensureCardWasClicked() {
        onView(withId(R.id.recycler_view))
                .check(matches(isDisplayed()))
                .perform(RecyclerViewActions.scrollToPosition(SCROLL_POSITION))
                .perform(RecyclerViewActions.actionOnItemAtPosition(SCROLL_POSITION, click()));

        intended(allOf(
                hasComponent(DetailActivity.class.getName()),
                hasExtraWithKey("githuber"))
        );
    }
}