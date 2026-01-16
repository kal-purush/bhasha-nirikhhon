package com.joymeter.utils;

import java.util.concurrent.TimeUnit;

/**
 * Created by hramovecchi on 24/11/2015.
 */
public class DurationUtils {

    private static DurationUtils instance;

    private DurationUtils() {
    }

    public static DurationUtils getInstance() {
        if (instance == null) {
            instance = new DurationUtils();
        }
        return instance;
    }

    public String getDuration(int hours, int minutes) {
        String min = (minutes < 10) ? "0" + String.valueOf(minutes) : String.valueOf(minutes);
        return hours + ":" + min;
    }

    public int getHours(long timeInMillis) {
        return (int) TimeUnit.MILLISECONDS.toHours(timeInMillis);
    }

    public int getMinutes(long timeInMillis) {
        return (int) (TimeUnit.MILLISECONDS.toMinutes(timeInMillis) -
                TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(timeInMillis)));
    }

    public long getEndDate(long initialDate, int hours, int minutes) {
        long endDate = (long) hours * 60 * 60 * 1000;
        endDate += minutes * 60 * 1000;
        return initialDate + endDate;
    }
}