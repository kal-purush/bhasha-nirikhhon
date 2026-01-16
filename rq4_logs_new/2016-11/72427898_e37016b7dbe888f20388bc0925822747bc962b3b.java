package com.hormiga6.androidapipractice;

import android.app.Activity;
import android.support.test.InstrumentationRegistry;
import android.support.test.filters.SmallTest;
import android.support.test.rule.ActivityTestRule;
import android.support.test.runner.AndroidJUnit4;
import android.support.test.runner.lifecycle.ActivityLifecycleMonitorRegistry;
import android.support.test.runner.lifecycle.Stage;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Collection;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by kotaro.arimura on 2016/11/22.
 */

@RunWith(AndroidJUnit4.class)
@SmallTest
public class PathTest {
    private static final String TAG = "FilePathTest";

    @Rule
    public ActivityTestRule<MainActivity> activityActivityTestRule = new ActivityTestRule<MainActivity>(MainActivity.class);

    @Test
    public void testRead(){
        assertThat(new File("").getAbsoluteFile().toString(), is("/"));
        assertThat(getCurrentActivity().getFilesDir().toString(), is("/data/user/0/com.hormiga6.androidapipractice/files"));
    }

    public static Activity getCurrentActivity() {
        InstrumentationRegistry.getInstrumentation().waitForIdleSync();
        final Activity[] activities = new Activity[1];
        InstrumentationRegistry.getInstrumentation().runOnMainSync(new Runnable() {
            @Override
            public void run() {
                Collection<Activity> activitiesInStage = ActivityLifecycleMonitorRegistry.getInstance().getActivitiesInStage(Stage.RESUMED);
                activities[0] = activitiesInStage.iterator().next();
            }
        });
        return activities[0];
    }
}