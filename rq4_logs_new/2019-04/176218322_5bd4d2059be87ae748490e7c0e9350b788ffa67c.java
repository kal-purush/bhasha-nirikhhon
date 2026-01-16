package com.example.lui_project;

import android.util.Log;
import android.content.Context;
import android.support.test.InstrumentationRegistry;
import android.support.test.annotation.UiThreadTest;
import android.support.test.rule.ActivityTestRule;
import android.support.test.runner.AndroidJUnit4;

import com.example.lui_project.utils.Constant;
import com.example.lui_project.utils.DateUtils;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Random;

import static org.junit.Assert.assertEquals;



/**
 * Instrumented test, which will execute on an Android device.
 *
 * @see <a href="http://d.android.com/tools/testing">Testing documentation</a>
 */
@RunWith(AndroidJUnit4.class)
public class SaveHealthyPlanTest {
    ActivityTestRule<SettingHealthyHealthyActivity> activityRule = new ActivityTestRule<>(SettingHealthyHealthyActivity.class);
    SettingHealthyHealthyActivity set_plan;
    @Before
    public void setUp() throws Exception {
        activityRule.launchActivity(null);
        set_plan = activityRule.getActivity();
    }
    @Test
    @UiThreadTest
    public void useAppContext() {
        // Context of the app under test.
        Context appContext = InstrumentationRegistry.getTargetContext();


        int type;
        String title_name;
        int start_year;
        int start_month;
        int start_day;
        int stop_year;
        int stop_month;
        int stop_day;
        Long nowTime;
        Long wantSaveTime;
        int isAdd_24_hours;
        int selectID;
        type = 0 ;//運動:飛鳥
        title_name = "俯身啞鈴飛鳥";
        start_year = 2019;
        start_month = 4;
        start_day = 7;
        stop_year = 2019;
        stop_month = 4;
        stop_day = 24;
        isAdd_24_hours = 0;
        nowTime = 1554686440000L;
        Random rand = new Random();
        wantSaveTime = DateUtils.getMillisecondValues( rand.nextInt(25), rand.nextInt(61));
        if (wantSaveTime <= nowTime ){
            wantSaveTime += Constant.DAY_FOR_24_HOURS;
            isAdd_24_hours = 1;
        }
        selectID = 0;
        Boolean isToSave = set_plan.contain_data(  type,  title_name,  start_year, start_month, start_day, stop_year, stop_month, stop_day,  nowTime ,  wantSaveTime,  isAdd_24_hours,  selectID );

        assertEquals(true, isToSave);
    }
}