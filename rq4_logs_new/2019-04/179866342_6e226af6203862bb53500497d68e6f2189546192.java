package com.deltorostudios.punchclockmockup2.Activities;

import android.support.test.rule.ActivityTestRule;

import com.deltorostudios.punchclockmockup2.R;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static android.support.test.espresso.Espresso.onView;
import static android.support.test.espresso.action.ViewActions.replaceText;
import static android.support.test.espresso.matcher.ViewMatchers.assertThat;
import static android.support.test.espresso.matcher.ViewMatchers.isChecked;
import static android.support.test.espresso.matcher.ViewMatchers.withId;

public class MainActivityTest {

    @Rule
    public ActivityTestRule<MainActivity> mainRule = new ActivityTestRule<MainActivity>(MainActivity.class);

    @Before
    public void init() {
        mainRule.getActivity().getSupportFragmentManager().beginTransaction();
    }

    @Test
    public void DoViewsExistTest() {
        onView(withId(R.id.testView)).perform(replaceText("GET MONEY"));
    }
}