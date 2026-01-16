package com.example.valentincomte.getup;

import android.provider.BaseColumns;

/**
 * Created by valentincomte on 24/11/2015.
 */
public final class AlarmContract {

    public AlarmContract() {}

    public static abstract class Alarm implements BaseColumns {
        public static final String TABLE_NAME = "alarm";
        public static final String COLUMN_NAME_ALARM_NAME = "name";
        public static final String COLUMN_NAME_ALARM_TIME_HOUR = "Hour";
        public static final String COLUMN_NAME_ALARM_TIME_MINUTE = "Minute";
        public static final String COLUMN_NAME_ALARM_REPEAT_DAYS = "days";
        public static final String COLUMN_NAME_ALARM_TONE = "alarmTone";
        public static final String COLUMN_NAME_ALARM_ENABLED = "isEnabled";
        public static final String COLUMN_NAME_ALARM_SHAKE = "shake";
        public static final String COLUMN_NAME_ALARM_NUMBER_SHAKES = "numberShakes";
        public static final String COLUMN_NAME_ALARM_TALK = "talk";
        public static final String COLUMN_NAME_ALARM_TALK_FILENAME = "talkFileName";
        public static final String COLUMN_NAME_ALARM_WALK = "walk";
        public static final String COLUMN_NAME_ALARM_NUMBER_STEPS = "numberSteps";
        public static final String COLUMN_NAME_ALARM_VIBRATE = "vibrations";
    }

}