package com.ketchuptime;

import android.content.Context;

/**
 * Created by Chase on 9/30/2016.
 */

public class Chronometer implements Runnable {

    public static final long MILLIS_TO_MINUTES = 60000;
    public static final long MILLIS_TO_HOURS = 3600000;

    private Context mContext;
    private long mStartTime;

    private boolean mIsRunning;

    public Chronometer(Context mContext) {
        this.mContext = mContext;
    }

    public void start() {
        mStartTime = System.currentTimeMillis();
        mIsRunning = true;
    }

    public void stop() {
        mIsRunning = false;
    }

    @Override
    public void run() {

        while (mIsRunning) {
            long since = System.currentTimeMillis() - mStartTime;

            int seconds = (int) ((since / 1000) % 60);
            int minutes = (int) ((since / MILLIS_TO_MINUTES) % 60);
            int hours = (int) ((since / MILLIS_TO_HOURS) % 24);
            int millis = (int) since % 1000;

            ((MainActivity)mContext).updateTimerText(String.format("%02d:%02d:%02d:%03d", hours, minutes, seconds, millis));
        }
    }
}