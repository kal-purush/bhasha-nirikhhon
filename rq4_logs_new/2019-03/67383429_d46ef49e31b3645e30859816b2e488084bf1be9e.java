package com.test.utils;

import java.util.*;

/**
 * Created by bj-s2-w1631 on 18-8-13.
 */
public class CommonUtil {
    public static String generateUUID() {
        return UUID.randomUUID().toString();
    }

    public static int getCurrentTimeStamp() {
        return (int) (System.currentTimeMillis() / 1000);
    }
    public static int getDayEndTime(int timePoint) {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT+8"));
        long theTime = ((long) timePoint) * 1000;
        calendar.setTimeInMillis(theTime);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);
        int second = calendar.get(Calendar.SECOND);
        calendar.clear();
        calendar.set(year, month, day, 23, 59, 59);
        int ret = (int) (calendar.getTimeInMillis() / 1000);
        return ret;
    }


    public static int getDayStartTime(int timePoint) {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT+8"));
        long theTime = ((long) timePoint) * 1000;
        calendar.setTimeInMillis(theTime);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);
        int second = calendar.get(Calendar.SECOND);
        calendar.clear();
        calendar.set(year, month, day, 0, 0, 0);
        int ret = (int) (calendar.getTimeInMillis() / 1000);
        return ret;
    }
}