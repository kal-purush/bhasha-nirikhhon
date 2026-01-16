package com.softdesign.school.school_softdesign.utils;

import android.util.Log;

/**
 * Created by Максим on 24.01.2016.
 */
public class Lg {


    /**
     * Зарефакторить код логера в соответствии с данными на лекции рекомендациями, исспользовать подход DRY Don’t repeat yourself (не повторяй себя) -
     * т.е. избегаем повторения уже ранее написанного кода + Javadoc,
     * логер должен исспользовать различные уровни вывода логов (Verbose, debug, info, error, warn, assert ).
     */


    private static final String PREFIX = "HTC ";
    public static final int LOGCAT_BUFFER_SIZE = 3000;

    private static boolean shouldLog() {
//        return BuildConfig.IS_LOGCAT_LOGGER_ENABLED;
//        return true;
        return false;
    }

    public static void i(String tag, String text) {
        if (shouldLog()) {
            if (text.length() > LOGCAT_BUFFER_SIZE) {
                String s = text;
                while (s.length() > LOGCAT_BUFFER_SIZE) {
                    String s1 = s.substring(0, LOGCAT_BUFFER_SIZE);
                    s = s.substring(LOGCAT_BUFFER_SIZE);
                    Log.i(PREFIX + tag, s1);
                }
                Log.i(PREFIX + tag, s);
            } else {
                Log.i(PREFIX + tag, text);
            }
        }
    }

    public static void e(String tag, String text) {
        if (shouldLog()) {
            if (text.length() > LOGCAT_BUFFER_SIZE) {
                String s = text;
                while (s.length() > LOGCAT_BUFFER_SIZE) {
                    String s1 = s.substring(0, LOGCAT_BUFFER_SIZE);
                    s = s.substring(LOGCAT_BUFFER_SIZE);
                    Log.e(PREFIX + tag, s1);
                }
                Log.e(PREFIX + tag, s);
            } else {
                Log.e(PREFIX + tag, text);
            }
        }
    }

    public static void w(String tag, String text) {
        if (shouldLog()) {
            if (text.length() > LOGCAT_BUFFER_SIZE) {
                String s = text;
                while (s.length() > LOGCAT_BUFFER_SIZE) {
                    String s1 = s.substring(0, LOGCAT_BUFFER_SIZE);
                    s = s.substring(LOGCAT_BUFFER_SIZE);
                    Log.w(PREFIX + tag, s1);
                }
                Log.w(PREFIX + tag, s);
            } else {
                Log.w(PREFIX + tag, text);
            }
        }
    }
}

