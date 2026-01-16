package com.sun.music_64.utils;

import java.util.concurrent.TimeUnit;

public class TimeConverters {
    public static String convertMilisecondToFormatTime(long milisecond) {
        return String.format("%02d:%02d",
                TimeUnit.MILLISECONDS.toMinutes(milisecond) -
                        TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(milisecond)),
                TimeUnit.MILLISECONDS.toSeconds(milisecond) -
                        TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(milisecond)));
    }
}