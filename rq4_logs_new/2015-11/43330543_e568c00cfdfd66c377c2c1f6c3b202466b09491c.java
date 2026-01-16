package com.leechangu.sweettask;

import android.support.test.espresso.Espresso;
import android.support.test.espresso.action.ViewActions;
import android.support.test.espresso.matcher.ViewMatchers;
import android.test.ActivityInstrumentationTestCase2;
import android.widget.SimpleAdapter;

/**
 * Created by CharlesGao on 15-11-03.
 */
public class checkCRUDTaskTest extends ActivityInstrumentationTestCase2<MainActivity> {

    public checkCRUDTaskTest() {
        super(MainActivity.class);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        getActivity();
    }

    public void testCreateARegularTask(){

        //Hit the create the button the ActionBar
        Espresso.onView(ViewMatchers.withId(R.id.menu_item_new)).perform(ViewActions.click());
        sleep();
        //Hit the content row to change the name of the task
        Espresso.onView(ViewMatchers.withText("Content")).perform(ViewActions.click());
        sleep();
        Espresso.onView(ViewMatchers.withId(R.id.et_task_pre_act_content)).perform(ViewActions.typeText("1"));
        sleep();
        Espresso.onView(ViewMatchers.withText("Ok")).perform(ViewActions.click());
        sleep();
        Espresso.onView(ViewMatchers.withId(R.id.menu_item_save)).perform(ViewActions.click());
        sleep();
        Espresso.onView(ViewMatchers.withText("1")).perform(ViewActions.longClick());
        sleep();
        Espresso.onView(ViewMatchers.withText("Edit")).perform(ViewActions.click());
        sleep();
        Espresso.onView(ViewMatchers.withId(R.id.menu_item_save)).perform(ViewActions.click());
        sleep();
        Espresso.onView(ViewMatchers.withText("1")).perform(ViewActions.longClick());
        sleep();
        Espresso.onView(ViewMatchers.withText("Delete")).perform(ViewActions.click());
        sleep();





    }



    public static void sleep(){
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}