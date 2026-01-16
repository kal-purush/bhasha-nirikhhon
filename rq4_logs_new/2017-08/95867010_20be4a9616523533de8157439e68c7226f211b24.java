package com.muyunluan.bakingapp;


import android.support.test.espresso.ViewInteraction;
import android.support.test.rule.ActivityTestRule;
import android.support.test.runner.AndroidJUnit4;
import android.test.suitebuilder.annotation.LargeTest;
import android.view.View;
import android.view.ViewGroup;
import android.view.ViewParent;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static android.support.test.espresso.Espresso.onView;
import static android.support.test.espresso.action.ViewActions.click;
import static android.support.test.espresso.assertion.ViewAssertions.matches;
import static android.support.test.espresso.contrib.RecyclerViewActions.actionOnItemAtPosition;
import static android.support.test.espresso.matcher.ViewMatchers.isDisplayed;
import static android.support.test.espresso.matcher.ViewMatchers.withId;
import static android.support.test.espresso.matcher.ViewMatchers.withText;
import static org.hamcrest.Matchers.allOf;

@LargeTest
@RunWith(AndroidJUnit4.class)
public class MainActivityTest {

    @Rule
    public ActivityTestRule<MainActivity> mActivityTestRule = new ActivityTestRule<>(MainActivity.class);

    @Test
    public void mainActivityTest2() {
        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ViewInteraction frameLayout = onView(
                allOf(withId(R.id.card_recipe),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.rv_recipes),
                                        0),
                                0),
                        isDisplayed()));
        frameLayout.check(matches(isDisplayed()));
        frameLayout.perform(click());

        ViewInteraction textView = onView(
                allOf(withId(R.id.tv_ingredients), withText("Ingredients: \nGraham Cracker crumbs ( 2 CUP )\nunsalted butter, melted ( 6 TBLSP )\ngranulated sugar ( 0 CUP )\nsalt ( 1 TSP )\nvanilla ( 5 TBLSP )\nNutella or other chocolate-hazelnut spread ( 1 K )\nMascapone Cheese(room temperature) ( 500 G )\nheavy cream(cold) ( 1 CUP )\ncream cheese(softened) ( 4 OZ )\n"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.recipe_details_frame),
                                        0),
                                0),
                        isDisplayed()));
        textView.check(matches(withText("Ingredients: \nGraham Cracker crumbs ( 2 CUP )\nunsalted butter, melted ( 6 TBLSP )\ngranulated sugar ( 0 CUP )\nsalt ( 1 TSP )\nvanilla ( 5 TBLSP )\nNutella or other chocolate-hazelnut spread ( 1 K )\nMascapone Cheese(room temperature) ( 500 G )\nheavy cream(cold) ( 1 CUP )\ncream cheese(softened) ( 4 OZ )\n")));

        ViewInteraction linearLayout = onView(
                allOf(childAtPosition(
                        allOf(withId(R.id.rv_steps_list),
                                childAtPosition(
                                        IsInstanceOf.<View>instanceOf(android.widget.LinearLayout.class),
                                        2)),
                        0),
                        isDisplayed()));
        linearLayout.check(matches(isDisplayed()));

        ViewInteraction recyclerView = onView(
                allOf(withId(R.id.rv_steps_list), isDisplayed()));
        recyclerView.perform(actionOnItemAtPosition(0, click()));

        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(4961);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ViewInteraction frameLayout2 = onView(
                allOf(withId(R.id.pv_step),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.stepslist),
                                        0),
                                0),
                        isDisplayed()));
        frameLayout2.check(matches(isDisplayed()));

        ViewInteraction button = onView(
                allOf(withId(R.id.bt_prev),
                        childAtPosition(
                                childAtPosition(
                                        IsInstanceOf.<View>instanceOf(android.widget.RelativeLayout.class),
                                        0),
                                0),
                        isDisplayed()));
        button.check(matches(isDisplayed()));

        ViewInteraction button2 = onView(
                allOf(withId(R.id.bt_next),
                        childAtPosition(
                                childAtPosition(
                                        IsInstanceOf.<View>instanceOf(android.widget.RelativeLayout.class),
                                        0),
                                1),
                        isDisplayed()));
        button2.check(matches(isDisplayed()));

    }

    private static Matcher<View> childAtPosition(
            final Matcher<View> parentMatcher, final int position) {

        return new TypeSafeMatcher<View>() {
            @Override
            public void describeTo(Description description) {
                description.appendText("Child at position " + position + " in parent ");
                parentMatcher.describeTo(description);
            }

            @Override
            public boolean matchesSafely(View view) {
                ViewParent parent = view.getParent();
                return parent instanceof ViewGroup && parentMatcher.matches(parent)
                        && view.equals(((ViewGroup) parent).getChildAt(position));
            }
        };
    }
}