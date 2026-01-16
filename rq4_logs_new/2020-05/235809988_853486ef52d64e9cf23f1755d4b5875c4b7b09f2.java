package com.group4sweng.scranplan;

import android.widget.Button;

import androidx.test.rule.ActivityTestRule;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

public class MainActivityTest {

    @Rule
    public ActivityTestRule<MainActivity> mActivityTestRule = new ActivityTestRule<MainActivity>(MainActivity.class);

    private MainActivity mActivity = null;

    @Before
    public void setUp() throws Exception {
        mActivity = mActivityTestRule.getActivity();
    }

    @Test
    public void onCreate() {
    }

    @Test
    public void onCreateOptionsMenu() {
    }

    @Test
    public void onOptionsItemSelected() {
    }

//    @Test
//    public void testLaunch(){
//        //Checking that the page is displaying the XML associated with the Login page
//        Button button = mActivity.findViewById(R.id.logoutButton);
//
//        assertNotNull(button);
//    }

    @After
    public void tearDown() throws Exception {

        mActivity = null;
    }
}