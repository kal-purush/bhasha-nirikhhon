package com.engapps.passwordlock;

import android.content.Context;
import android.content.SharedPreferences;
import android.preference.PreferenceManager;
import android.support.v4.app.FragmentManager;
import android.support.v4.app.FragmentTransaction;
import android.support.v7.app.AppCompatActivity;

public class AppLock {
    private static AppLock mInstance;

    private long mLastPause = 0;
    private int mFailedAttempts = 0;

    private boolean mIsLocked = false;
    private boolean mSkipLock = false;

    private PasswordDialogFragment mPasswordDialogFragment;

    private AppLock() {
    }

    public static AppLock getInstance() {
        if (mInstance == null) {
            mInstance = new AppLock();
        }
        return mInstance;
    }

    public void onPause() {
        mLastPause = System.currentTimeMillis();
    }

    public void lockIfNeeded(Context context) {
        if (mSkipLock) return;

        /* Only show the dialog if more than TIMEOUT has elapsed and if user has enabled password protection */
        if (mIsLocked || (System.currentTimeMillis() - mLastPause > getTimeout(context) && isPasswordEnabled(context))) {
            /* We are currently locked! */
            mIsLocked = true;
            mFailedAttempts = 0;

            /* Build the dialog if not already visible */
            FragmentManager fragmentManager = ((AppCompatActivity) context).getSupportFragmentManager();
            final String PASSWORD_DIALOG_TAG = "password_fragment";
            if (mPasswordDialogFragment == null || fragmentManager.findFragmentByTag(PASSWORD_DIALOG_TAG) == null) {
                mPasswordDialogFragment = new PasswordDialogFragment();
                FragmentTransaction transaction = fragmentManager.beginTransaction();
                transaction.add(android.R.id.content, mPasswordDialogFragment, PASSWORD_DIALOG_TAG).commitAllowingStateLoss();
            }
        }
    }

    private boolean isPasswordEnabled(Context context) {
        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(context);
        return prefs.getBoolean(context.getString(R.string.pref_password_enabled), false);
    }

    /**
     * Get the timeout in seconds from preferences and return in milliseconds
     *
     * @param context Application context
     * @return Timeout in milliseconds
     */
    private int getTimeout(Context context) {
        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(context);
        return Integer.valueOf(prefs.getString(context.getString(R.string.pref_timeout), "5")) * 1000;
    }

    public void onSuccessfulAttempt() {
        mIsLocked = false;
        mPasswordDialogFragment = null;
    }

    public void onFailedAttempt() {
        mFailedAttempts++;
    }

    public int getNumFailedAttempts() {
        return mFailedAttempts;
    }

    /**
     * Allows you to disable lock checking in case you want to allow exiting and returning to the
     * app in a trusted way. An example would be launching the camera then returning to the app.
     *
     * @param skipLock Whether to enable skipping the lock or not.
     */
    public void setSkipLock(boolean skipLock) {
        mSkipLock = skipLock;
    }
}