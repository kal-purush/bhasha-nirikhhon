package com.First.ISportsC.ui;

import android.content.Context;
import android.support.test.InstrumentationRegistry;
import android.support.test.espresso.ViewInteraction;
import android.view.View;

import androidx.test.rule.ActivityTestRule;

import com.First.ISportsC.R;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static android.support.test.espresso.Espresso.onView;
import static android.support.test.espresso.action.ViewActions.click;
import static android.support.test.espresso.matcher.ViewMatchers.isDisplayed;
import static android.support.test.espresso.matcher.ViewMatchers.withId;
import static android.support.test.espresso.matcher.ViewMatchers.withText;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.*;

public class MainActivityTest {

    @Test
    public void useAppContext()throws Exception{
        Context appContext = InstrumentationRegistry.getTargetContext();
        assertEquals("com.firstProject.ISportsC", appContext.getPackageName());

    }
    @Test
    public void mainActivity() {
        ViewInteraction appCompatButton = onView(
                allOf(withId(R.id.find_location), withText("Find Location"), isDisplayed()));
        appCompatButton.perform(click());
    }
}