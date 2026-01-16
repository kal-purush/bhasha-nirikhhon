package com.example.cs121.flarevisualizer;


import android.support.test.espresso.DataInteraction;
import android.support.test.espresso.ViewInteraction;
import android.support.test.filters.LargeTest;
import android.support.test.rule.ActivityTestRule;
import android.support.test.runner.AndroidJUnit4;
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

import static android.support.test.espresso.Espresso.onData;
import static android.support.test.espresso.Espresso.onView;
import static android.support.test.espresso.Espresso.pressBack;
import static android.support.test.espresso.action.ViewActions.click;
import static android.support.test.espresso.action.ViewActions.closeSoftKeyboard;
import static android.support.test.espresso.action.ViewActions.pressImeActionButton;
import static android.support.test.espresso.action.ViewActions.replaceText;
import static android.support.test.espresso.action.ViewActions.scrollTo;
import static android.support.test.espresso.assertion.ViewAssertions.matches;
import static android.support.test.espresso.matcher.ViewMatchers.isDisplayed;
import static android.support.test.espresso.matcher.ViewMatchers.withClassName;
import static android.support.test.espresso.matcher.ViewMatchers.withContentDescription;
import static android.support.test.espresso.matcher.ViewMatchers.withId;
import static android.support.test.espresso.matcher.ViewMatchers.withText;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.is;

@LargeTest
@RunWith(AndroidJUnit4.class)
public class EditAndListActivityTest {

    @Rule
    public ActivityTestRule<MainActivity> mActivityTestRule = new ActivityTestRule<>(MainActivity.class);

    @Test
    public void editAndListActivityTest() {
        ViewInteraction appCompatImageButton = onView(
                allOf(withContentDescription("Open navigation drawer"),
                        childAtPosition(
                                allOf(withId(R.id.toolbar),
                                        childAtPosition(
                                                withId(R.id.app_bar),
                                                0)),
                                1),
                        isDisplayed()));
        appCompatImageButton.perform(click());

        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ViewInteraction navigationMenuItemView = onView(
                allOf(childAtPosition(
                        allOf(withId(R.id.design_navigation_view),
                                childAtPosition(
                                        withId(R.id.nav_view),
                                        0)),
                        5),
                        isDisplayed()));
        navigationMenuItemView.perform(click());

        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ViewInteraction spinner = onView(
                allOf(withId(R.id.monthSpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                1),
                        isDisplayed()));
        spinner.check(matches(isDisplayed()));

        ViewInteraction spinner2 = onView(
                allOf(withId(R.id.daySpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                3),
                        isDisplayed()));
        spinner2.check(matches(isDisplayed()));

        ViewInteraction spinner3 = onView(
                allOf(withId(R.id.yearSpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                5),
                        isDisplayed()));
        spinner3.check(matches(isDisplayed()));

        ViewInteraction spinner4 = onView(
                allOf(withId(R.id.hourSpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                7),
                        isDisplayed()));
        spinner4.check(matches(isDisplayed()));

        ViewInteraction spinner5 = onView(
                allOf(withId(R.id.meridiemSpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                9),
                        isDisplayed()));
        spinner5.check(matches(isDisplayed()));

        ViewInteraction checkBox = onView(
                allOf(withId(R.id.endFlare),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                10),
                        isDisplayed()));
        checkBox.check(matches(isDisplayed()));

        ViewInteraction spinner6 = onView(
                allOf(withId(R.id.painRatingSpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                12),
                        isDisplayed()));
        spinner6.check(matches(isDisplayed()));

        ViewInteraction button = onView(
                allOf(withId(R.id.addTriggerBtn),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                13),
                        isDisplayed()));
        button.check(matches(isDisplayed()));

        ViewInteraction button2 = onView(
                allOf(withId(R.id.newFlareBtn),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                15),
                        isDisplayed()));
        button2.check(matches(isDisplayed()));

        ViewInteraction button3 = onView(
                allOf(withId(R.id.updateFlareBtn),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                16),
                        isDisplayed()));
        button3.check(matches(isDisplayed()));

        ViewInteraction scrollView = onView(
                allOf(childAtPosition(
                        childAtPosition(
                                withId(R.id.content_frame),
                                0),
                        14),
                        isDisplayed()));
        scrollView.check(matches(isDisplayed()));

        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ViewInteraction appCompatSpinner = onView(
                allOf(withId(R.id.monthSpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                1),
                        isDisplayed()));
        appCompatSpinner.perform(click());

        DataInteraction appCompatTextView = onData(anything())
                .inAdapterView(withClassName(is("android.support.v7.widget.DropDownListView")))
                .atPosition(10);
        appCompatTextView.perform(click());

        ViewInteraction appCompatSpinner2 = onView(
                allOf(withId(R.id.daySpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                3),
                        isDisplayed()));
        appCompatSpinner2.perform(click());

        DataInteraction appCompatTextView2 = onData(anything())
                .inAdapterView(withClassName(is("android.support.v7.widget.DropDownListView")))
                .atPosition(13);
        appCompatTextView2.perform(click());

        ViewInteraction appCompatSpinner3 = onView(
                allOf(withId(R.id.yearSpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                5),
                        isDisplayed()));
        appCompatSpinner3.perform(click());

        DataInteraction appCompatTextView3 = onData(anything())
                .inAdapterView(withClassName(is("android.support.v7.widget.DropDownListView")))
                .atPosition(8);
        appCompatTextView3.perform(click());

        ViewInteraction appCompatSpinner4 = onView(
                allOf(withId(R.id.hourSpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                7),
                        isDisplayed()));
        appCompatSpinner4.perform(click());

        DataInteraction appCompatTextView4 = onData(anything())
                .inAdapterView(withClassName(is("android.support.v7.widget.DropDownListView")))
                .atPosition(4);
        appCompatTextView4.perform(click());

        ViewInteraction appCompatSpinner5 = onView(
                allOf(withId(R.id.meridiemSpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                9),
                        isDisplayed()));
        appCompatSpinner5.perform(click());

        DataInteraction appCompatTextView5 = onData(anything())
                .inAdapterView(withClassName(is("android.support.v7.widget.DropDownListView")))
                .atPosition(1);
        appCompatTextView5.perform(click());

        ViewInteraction appCompatCheckBox = onView(
                allOf(withId(R.id.endFlare), withText("End Flare"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                22),
                        isDisplayed()));
        appCompatCheckBox.perform(click());

        ViewInteraction appCompatSpinner6 = onView(
                allOf(withId(R.id.painRatingSpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                11),
                        isDisplayed()));
        appCompatSpinner6.perform(click());

        DataInteraction appCompatTextView6 = onData(anything())
                .inAdapterView(withClassName(is("android.support.v7.widget.DropDownListView")))
                .atPosition(3);
        appCompatTextView6.perform(click());

        ViewInteraction appCompatButton = onView(
                allOf(withId(R.id.addTriggerBtn), withText("Add Trigger"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                20),
                        isDisplayed()));
        appCompatButton.perform(click());

        ViewInteraction appCompatButton2 = onView(
                allOf(withId(R.id.addTriggerBtn), withText("Add Trigger"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                20),
                        isDisplayed()));
        appCompatButton2.perform(click());

        ViewInteraction appCompatButton3 = onView(
                allOf(withId(R.id.addTriggerBtn), withText("Add Trigger"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                20),
                        isDisplayed()));
        appCompatButton3.perform(click());

        ViewInteraction appCompatSpinner7 = onView(
                allOf(withId(R.id.triggerSpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.editTriggerLayout),
                                        1),
                                0)));
        appCompatSpinner7.perform(scrollTo(), click());

        DataInteraction appCompatTextView7 = onData(anything())
                .inAdapterView(withClassName(is("android.support.v7.widget.DropDownListView")))
                .atPosition(1);
        appCompatTextView7.perform(click());

        ViewInteraction appCompatSpinner8 = onView(
                allOf(withId(R.id.triggerSpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.editTriggerLayout),
                                        2),
                                0)));
        appCompatSpinner8.perform(scrollTo(), click());

        DataInteraction appCompatTextView8 = onData(anything())
                .inAdapterView(withClassName(is("android.support.v7.widget.DropDownListView")))
                .atPosition(2);
        appCompatTextView8.perform(click());

        ViewInteraction appCompatCheckBox2 = onView(
                allOf(withId(R.id.endFlare), withText("End Flare"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                22),
                        isDisplayed()));
        appCompatCheckBox2.perform(click());

        ViewInteraction appCompatEditText = onView(
                allOf(withId(R.id.triggerName),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.editTriggerLayout),
                                        0),
                                1)));
        appCompatEditText.perform(scrollTo(), replaceText("a"), closeSoftKeyboard());

        ViewInteraction appCompatEditText2 = onView(
                allOf(withId(R.id.triggerName), withText("a"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.editTriggerLayout),
                                        0),
                                1)));
        appCompatEditText2.perform(pressImeActionButton());

        ViewInteraction appCompatEditText3 = onView(
                allOf(withId(R.id.triggerName),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.editTriggerLayout),
                                        1),
                                1)));
        appCompatEditText3.perform(scrollTo(), replaceText("b"), closeSoftKeyboard());

        ViewInteraction appCompatEditText4 = onView(
                allOf(withId(R.id.triggerName), withText("b"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.editTriggerLayout),
                                        1),
                                1)));
        appCompatEditText4.perform(pressImeActionButton());

        ViewInteraction appCompatEditText5 = onView(
                allOf(withId(R.id.triggerName),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.editTriggerLayout),
                                        2),
                                1)));
        appCompatEditText5.perform(scrollTo(), replaceText("c"), closeSoftKeyboard());

        ViewInteraction appCompatEditText6 = onView(
                allOf(withId(R.id.triggerName), withText("c"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.editTriggerLayout),
                                        2),
                                1)));
        appCompatEditText6.perform(pressImeActionButton());

        ViewInteraction appCompatButton4 = onView(
                allOf(withId(R.id.addTriggerBtn), withText("Add Trigger"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                20),
                        isDisplayed()));
        appCompatButton4.perform(click());

        ViewInteraction appCompatButton5 = onView(
                allOf(withId(R.id.cancelTriggerBtn), withText("X"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.editTriggerLayout),
                                        3),
                                2)));
        appCompatButton5.perform(scrollTo(), click());

        ViewInteraction appCompatButton6 = onView(
                allOf(withId(R.id.newFlareBtn), withText("Add Flare"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                12),
                        isDisplayed()));
        appCompatButton6.perform(click());

        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ViewInteraction appCompatImageButton2 = onView(
                allOf(withContentDescription("Open navigation drawer"),
                        childAtPosition(
                                allOf(withId(R.id.toolbar),
                                        childAtPosition(
                                                withId(R.id.app_bar),
                                                0)),
                                1),
                        isDisplayed()));
        appCompatImageButton2.perform(click());

        ViewInteraction navigationMenuItemView2 = onView(
                allOf(childAtPosition(
                        allOf(withId(R.id.design_navigation_view),
                                childAtPosition(
                                        withId(R.id.nav_view),
                                        0)),
                        5),
                        isDisplayed()));
        navigationMenuItemView2.perform(click());

        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ViewInteraction appCompatSpinner9 = onView(
                allOf(withId(R.id.monthSpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                1),
                        isDisplayed()));
        appCompatSpinner9.perform(click());

        DataInteraction appCompatTextView9 = onData(anything())
                .inAdapterView(withClassName(is("android.support.v7.widget.DropDownListView")))
                .atPosition(12);
        appCompatTextView9.perform(click());

        ViewInteraction appCompatSpinner10 = onView(
                allOf(withId(R.id.daySpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                3),
                        isDisplayed()));
        appCompatSpinner10.perform(click());

        ViewInteraction appCompatSpinner11 = onView(
                allOf(withId(R.id.daySpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                3),
                        isDisplayed()));
        appCompatSpinner11.perform(click());

        DataInteraction appCompatTextView10 = onData(anything())
                .inAdapterView(withClassName(is("android.support.v7.widget.DropDownListView")))
                .atPosition(29);
        appCompatTextView10.perform(click());

        ViewInteraction appCompatSpinner12 = onView(
                allOf(withId(R.id.yearSpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                5),
                        isDisplayed()));
        appCompatSpinner12.perform(click());

        DataInteraction appCompatTextView11 = onData(anything())
                .inAdapterView(withClassName(is("android.support.v7.widget.DropDownListView")))
                .atPosition(1);
        appCompatTextView11.perform(click());

        ViewInteraction appCompatSpinner13 = onView(
                allOf(withId(R.id.hourSpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                7),
                        isDisplayed()));
        appCompatSpinner13.perform(click());

        DataInteraction appCompatTextView12 = onData(anything())
                .inAdapterView(withClassName(is("android.support.v7.widget.DropDownListView")))
                .atPosition(6);
        appCompatTextView12.perform(click());

        ViewInteraction appCompatSpinner14 = onView(
                allOf(withId(R.id.meridiemSpinner),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                9),
                        isDisplayed()));
        appCompatSpinner14.perform(click());

        DataInteraction appCompatTextView13 = onData(anything())
                .inAdapterView(withClassName(is("android.support.v7.widget.DropDownListView")))
                .atPosition(1);
        appCompatTextView13.perform(click());

        ViewInteraction appCompatCheckBox3 = onView(
                allOf(withId(R.id.endFlare), withText("End Flare"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                22),
                        isDisplayed()));
        appCompatCheckBox3.perform(click());

        ViewInteraction appCompatButton7 = onView(
                allOf(withId(R.id.addTriggerBtn), withText("Add Trigger"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                20),
                        isDisplayed()));
        appCompatButton7.perform(click());

        ViewInteraction appCompatEditText7 = onView(
                allOf(withId(R.id.triggerName),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.editTriggerLayout),
                                        0),
                                1)));
        appCompatEditText7.perform(scrollTo(), replaceText("a"), closeSoftKeyboard());

        ViewInteraction appCompatEditText8 = onView(
                allOf(withId(R.id.triggerName), withText("a"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.editTriggerLayout),
                                        0),
                                1)));
        appCompatEditText8.perform(pressImeActionButton());

        ViewInteraction appCompatButton8 = onView(
                allOf(withId(R.id.updateFlareBtn), withText("Update Flare"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                13),
                        isDisplayed()));
        appCompatButton8.perform(click());

        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ViewInteraction appCompatImageButton3 = onView(
                allOf(withContentDescription("Open navigation drawer"),
                        childAtPosition(
                                allOf(withId(R.id.toolbar),
                                        childAtPosition(
                                                withId(R.id.app_bar),
                                                0)),
                                1),
                        isDisplayed()));
        appCompatImageButton3.perform(click());

        ViewInteraction navigationMenuItemView3 = onView(
                allOf(childAtPosition(
                        allOf(withId(R.id.design_navigation_view),
                                childAtPosition(
                                        withId(R.id.nav_view),
                                        0)),
                        2),
                        isDisplayed()));
        navigationMenuItemView3.perform(click());

        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ViewInteraction textView = onView(
                allOf(withId(R.id.triggerListHeader), withText("Diet List"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                0),
                        isDisplayed()));
        textView.check(matches(withText("Diet List")));

        ViewInteraction listView = onView(
                allOf(withId(R.id.triggerList),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                1),
                        isDisplayed()));
        listView.check(matches(isDisplayed()));

        ViewInteraction textView2 = onView(
                allOf(withId(android.R.id.text1), withText("b"),
                        childAtPosition(
                                allOf(withId(R.id.triggerList),
                                        childAtPosition(
                                                IsInstanceOf.<View>instanceOf(android.widget.LinearLayout.class),
                                                1)),
                                2),
                        isDisplayed()));
        textView2.check(matches(withText("b")));

        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ViewInteraction appCompatButton9 = onView(
                allOf(withId(android.R.id.button2), withText("NO"),
                        childAtPosition(
                                allOf(withClassName(is("android.widget.LinearLayout")),
                                        childAtPosition(
                                                withClassName(is("android.widget.LinearLayout")),
                                                3)),
                                2),
                        isDisplayed()));
        appCompatButton9.perform(click());

        DataInteraction appCompatTextView14 = onData(anything())
                .inAdapterView(allOf(withId(R.id.triggerList),
                        childAtPosition(
                                withClassName(is("android.widget.LinearLayout")),
                                1)))
                .atPosition(2);
        appCompatTextView14.perform(click());

        ViewInteraction appCompatEditText9 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("b"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText9.perform(click());

        ViewInteraction appCompatEditText10 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("b"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText10.perform(replaceText("c"));

        ViewInteraction appCompatEditText11 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("c"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText11.perform(closeSoftKeyboard());

        ViewInteraction appCompatButton10 = onView(
                allOf(withId(R.id.cancel_trigger_button), withText("Cancel"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                2),
                        isDisplayed()));
        appCompatButton10.perform(click());

        DataInteraction appCompatTextView15 = onData(anything())
                .inAdapterView(allOf(withId(R.id.triggerList),
                        childAtPosition(
                                withClassName(is("android.widget.LinearLayout")),
                                1)))
                .atPosition(2);
        appCompatTextView15.perform(click());

        ViewInteraction appCompatEditText12 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("b"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText12.perform(click());

        ViewInteraction appCompatEditText13 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("b"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText13.perform(replaceText("e"));

        ViewInteraction appCompatEditText14 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("e"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText14.perform(closeSoftKeyboard());

        ViewInteraction appCompatButton11 = onView(
                allOf(withId(R.id.update_trigger_button), withText("Update"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                1),
                        isDisplayed()));
        appCompatButton11.perform(click());

        ViewInteraction appCompatImageButton4 = onView(
                allOf(withContentDescription("Open navigation drawer"),
                        childAtPosition(
                                allOf(withId(R.id.toolbar),
                                        childAtPosition(
                                                withId(R.id.app_bar),
                                                0)),
                                1),
                        isDisplayed()));
        appCompatImageButton4.perform(click());

        ViewInteraction navigationMenuItemView4 = onView(
                allOf(childAtPosition(
                        allOf(withId(R.id.design_navigation_view),
                                childAtPosition(
                                        withId(R.id.nav_view),
                                        0)),
                        4),
                        isDisplayed()));
        navigationMenuItemView4.perform(click());

        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ViewInteraction textView3 = onView(
                allOf(withId(R.id.triggerListHeader), withText("Miscellany List"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                0),
                        isDisplayed()));
        textView3.check(matches(withText("Miscellany List")));

        ViewInteraction listView2 = onView(
                allOf(withId(R.id.triggerList),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                1),
                        isDisplayed()));
        listView2.check(matches(isDisplayed()));

        ViewInteraction textView4 = onView(
                allOf(withId(android.R.id.text1), withText("c"),
                        childAtPosition(
                                allOf(withId(R.id.triggerList),
                                        childAtPosition(
                                                IsInstanceOf.<View>instanceOf(android.widget.LinearLayout.class),
                                                1)),
                                2),
                        isDisplayed()));
        textView4.check(matches(withText("c")));

        DataInteraction appCompatTextView16 = onData(anything())
                .inAdapterView(allOf(withId(R.id.triggerList),
                        childAtPosition(
                                withClassName(is("android.widget.LinearLayout")),
                                1)))
                .atPosition(2);
        appCompatTextView16.perform(click());

        ViewInteraction appCompatEditText15 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("c"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText15.perform(click());

        ViewInteraction appCompatEditText16 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("c"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText16.perform(replaceText("f"));

        ViewInteraction appCompatEditText17 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("f"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText17.perform(closeSoftKeyboard());

        ViewInteraction appCompatButton12 = onView(
                allOf(withId(R.id.cancel_trigger_button), withText("Cancel"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                2),
                        isDisplayed()));
        appCompatButton12.perform(click());

        DataInteraction appCompatTextView17 = onData(anything())
                .inAdapterView(allOf(withId(R.id.triggerList),
                        childAtPosition(
                                withClassName(is("android.widget.LinearLayout")),
                                1)))
                .atPosition(2);
        appCompatTextView17.perform(click());

        ViewInteraction appCompatEditText18 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("c"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText18.perform(click());

        ViewInteraction appCompatEditText19 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("c"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText19.perform(replaceText("f"));

        ViewInteraction appCompatEditText20 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("f"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText20.perform(closeSoftKeyboard());

        ViewInteraction appCompatButton13 = onView(
                allOf(withId(R.id.update_trigger_button), withText("Update"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                1),
                        isDisplayed()));
        appCompatButton13.perform(click());

        ViewInteraction appCompatButton14 = onView(
                allOf(withId(android.R.id.button2), withText("NO"),
                        childAtPosition(
                                allOf(withClassName(is("android.widget.LinearLayout")),
                                        childAtPosition(
                                                withClassName(is("android.widget.LinearLayout")),
                                                3)),
                                2),
                        isDisplayed()));
        appCompatButton14.perform(click());

        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        DataInteraction appCompatTextView18 = onData(anything())
                .inAdapterView(allOf(withId(R.id.triggerList),
                        childAtPosition(
                                withClassName(is("android.widget.LinearLayout")),
                                1)))
                .atPosition(2);
        appCompatTextView18.perform(click());

        ViewInteraction appCompatButton15 = onView(
                allOf(withId(R.id.cancel_trigger_button), withText("Cancel"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                2),
                        isDisplayed()));
        appCompatButton15.perform(click());

        ViewInteraction appCompatButton16 = onView(
                allOf(withId(android.R.id.button1), withText("YES"),
                        childAtPosition(
                                allOf(withClassName(is("android.widget.LinearLayout")),
                                        childAtPosition(
                                                withClassName(is("android.widget.LinearLayout")),
                                                3)),
                                3),
                        isDisplayed()));
        appCompatButton16.perform(click());

        ViewInteraction appCompatImageButton5 = onView(
                allOf(withContentDescription("Open navigation drawer"),
                        childAtPosition(
                                allOf(withId(R.id.toolbar),
                                        childAtPosition(
                                                withId(R.id.app_bar),
                                                0)),
                                1),
                        isDisplayed()));
        appCompatImageButton5.perform(click());

        ViewInteraction navigationMenuItemView5 = onView(
                allOf(childAtPosition(
                        allOf(withId(R.id.design_navigation_view),
                                childAtPosition(
                                        withId(R.id.nav_view),
                                        0)),
                        3),
                        isDisplayed()));
        navigationMenuItemView5.perform(click());

        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ViewInteraction textView10 = onView(
                allOf(withId(R.id.triggerListHeader), withText("Activity List"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                0),
                        isDisplayed()));
        textView10.check(matches(withText("Activity List")));

        ViewInteraction listView10 = onView(
                allOf(withId(R.id.triggerList),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                1),
                        isDisplayed()));
        listView10.check(matches(isDisplayed()));

        ViewInteraction textView11 = onView(
                allOf(withId(android.R.id.text1), withText("a"),
                        childAtPosition(
                                allOf(withId(R.id.triggerList),
                                        childAtPosition(
                                                IsInstanceOf.<View>instanceOf(android.widget.LinearLayout.class),
                                                1)),
                                2),
                        isDisplayed()));
        textView11.check(matches(withText("a")));

        DataInteraction appCompatTextView19 = onData(anything())
                .inAdapterView(allOf(withId(R.id.triggerList),
                        childAtPosition(
                                withClassName(is("android.widget.LinearLayout")),
                                1)))
                .atPosition(2);
        appCompatTextView19.perform(click());

        ViewInteraction appCompatEditText21 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("a"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText21.perform(click());

        ViewInteraction appCompatEditText22 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("a"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText22.perform(replaceText("g"));

        ViewInteraction appCompatEditText23 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("g"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText23.perform(closeSoftKeyboard());

        ViewInteraction appCompatButton17 = onView(
                allOf(withId(R.id.cancel_trigger_button), withText("Cancel"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                2),
                        isDisplayed()));
        appCompatButton17.perform(click());

        DataInteraction appCompatTextView20 = onData(anything())
                .inAdapterView(allOf(withId(R.id.triggerList),
                        childAtPosition(
                                withClassName(is("android.widget.LinearLayout")),
                                1)))
                .atPosition(2);
        appCompatTextView20.perform(click());

        ViewInteraction appCompatEditText24 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("a"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText24.perform(click());

        ViewInteraction appCompatEditText25 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("a"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText25.perform(replaceText("g"));

        ViewInteraction appCompatEditText26 = onView(
                allOf(withId(R.id.edit_trigger_text), withText("g"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                0),
                        isDisplayed()));
        appCompatEditText26.perform(closeSoftKeyboard());

        ViewInteraction appCompatButton18 = onView(
                allOf(withId(R.id.update_trigger_button), withText("Update"),
                        childAtPosition(
                                childAtPosition(
                                        withId(android.R.id.custom),
                                        0),
                                1),
                        isDisplayed()));
        appCompatButton18.perform(click());

        ViewInteraction appCompatButton19 = onView(
                allOf(withId(android.R.id.button2), withText("NO"),
                        childAtPosition(
                                allOf(withClassName(is("android.widget.LinearLayout")),
                                        childAtPosition(
                                                withClassName(is("android.widget.LinearLayout")),
                                                3)),
                                2),
                        isDisplayed()));
        appCompatButton19.perform(click());

        ViewInteraction appCompatButton20 = onView(
                allOf(withId(android.R.id.button1), withText("YES"),
                        childAtPosition(
                                allOf(withClassName(is("android.widget.LinearLayout")),
                                        childAtPosition(
                                                withClassName(is("android.widget.LinearLayout")),
                                                3)),
                                3),
                        isDisplayed()));
        appCompatButton20.perform(click());


        ViewInteraction appCompatImageButton6 = onView(
                allOf(withContentDescription("Open navigation drawer"),
                        childAtPosition(
                                allOf(withId(R.id.toolbar),
                                        childAtPosition(
                                                withId(R.id.app_bar),
                                                0)),
                                1),
                        isDisplayed()));
        appCompatImageButton6.perform(click());

        ViewInteraction navigationMenuItemView6 = onView(
                allOf(childAtPosition(
                        allOf(withId(R.id.design_navigation_view),
                                childAtPosition(
                                        withId(R.id.nav_view),
                                        0)),
                        6),
                        isDisplayed()));
        navigationMenuItemView6.perform(click());

        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ViewInteraction textView6 = onView(
                allOf(withId(R.id.triggerListHeader), withText("Flares List"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                0),
                        isDisplayed()));
        textView6.check(matches(withText("Flares List")));

        ViewInteraction listView4 = onView(
                allOf(withId(R.id.triggerList),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                1),
                        isDisplayed()));
        listView4.check(matches(isDisplayed()));

        ViewInteraction textView7 = onView(
                allOf(withId(android.R.id.text1), withText("flare0"),
                        childAtPosition(
                                allOf(withId(R.id.triggerList),
                                        childAtPosition(
                                                IsInstanceOf.<View>instanceOf(android.widget.LinearLayout.class),
                                                1)),
                                0),
                        isDisplayed()));
        textView7.check(matches(withText("flare0")));

        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        DataInteraction appCompatTextView21 = onData(anything())
                .inAdapterView(allOf(withId(R.id.triggerList),
                        childAtPosition(
                                withClassName(is("android.widget.LinearLayout")),
                                1)))
                .atPosition(0);
        appCompatTextView21.perform(click());

        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ViewInteraction textView8 = onView(
                allOf(withId(R.id.triggerListHeader), withText("flare0 List"),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                0),
                        isDisplayed()));
        textView8.check(matches(withText("flare0 List")));

        ViewInteraction listView5 = onView(
                allOf(withId(R.id.triggerList),
                        childAtPosition(
                                childAtPosition(
                                        withId(R.id.content_frame),
                                        0),
                                1),
                        isDisplayed()));
        listView5.check(matches(isDisplayed()));

        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        pressBack();

        // Added a sleep statement to match the app's execution delay.
        // The recommended way to handle such scenarios is to use Espresso idling resources:
        // https://google.github.io/android-testing-support-library/docs/espresso/idling-resource/index.html
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ViewInteraction appCompatButton21 = onView(
                allOf(withId(android.R.id.button1), withText("YES"),
                        childAtPosition(
                                allOf(withClassName(is("android.widget.LinearLayout")),
                                        childAtPosition(
                                                withClassName(is("android.widget.LinearLayout")),
                                                3)),
                                3),
                        isDisplayed()));
        appCompatButton21.perform(click());
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