package com.tline.android;


import android.support.test.espresso.ViewInteraction;
import android.support.test.rule.ActivityTestRule;
import android.support.test.runner.AndroidJUnit4;
import android.test.suitebuilder.annotation.LargeTest;

import com.tline.android.features.timeline.activity.view.impl.TimelineActivity;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static android.support.test.espresso.Espresso.onView;
import static android.support.test.espresso.assertion.ViewAssertions.matches;
import static android.support.test.espresso.matcher.ViewMatchers.hasDescendant;
import static android.support.test.espresso.matcher.ViewMatchers.isDisplayed;
import static android.support.test.espresso.matcher.ViewMatchers.withId;

@LargeTest
@RunWith(AndroidJUnit4.class)
public class TimelineActivityTest {

    @Rule
    public ActivityTestRule<TimelineActivity> mActivityTestRule = new ActivityTestRule<>(TimelineActivity.class);


    @Before
    public void setUp() throws Exception {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void checkActionbarItemsVisiblility(){
        ViewInteraction language = onView(withId(R.id.action_language));
        ViewInteraction logout = onView(withId(R.id.action_logout)).perform();

        language.check(matches(isDisplayed()));
        logout.check(matches(isDisplayed()));
    }

    @Test
    public void checkTimelineBottomTabVisiblility(){
        ViewInteraction tab = onView(withId(R.id.action_timeline));

        tab.check(matches(isDisplayed()));
    }

    @Test
    public void checkAndroidDevBottomTabVisiblility(){
        ViewInteraction tab = onView(withId(R.id.action_android_dev));

        tab.check(matches(isDisplayed()));
    }

    @Test
    public void checkOlxBottomTabVisiblility(){
        ViewInteraction tab = onView(withId(R.id.action_olx_egypt));

        tab.check(matches(isDisplayed()));
    }

    @Test
    public void checkRecyclerViewVisibility(){

        ViewInteraction scroll = onView(withId(R.id.recyclerView));

        scroll.check(matches(isDisplayed()));
    }

    @Test
    public void checkScrollViewHasAnyItem(){

        ViewInteraction container = onView(withId(R.id.recyclerView));

        container.check(matches(hasDescendant(withId(R.id.tvUserName))));
    }
}