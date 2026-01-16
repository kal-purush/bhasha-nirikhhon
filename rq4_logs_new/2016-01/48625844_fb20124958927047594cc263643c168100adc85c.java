package com.blstreampatronage.patrykkrawczyk;

import android.app.Activity;
import android.app.Application;
import android.os.StrictMode;

import com.squareup.leakcanary.LeakCanary;

/**
 * Class used to initialize debug helpers for developer
 * Consists of LeakCanary and StrictMode
 * Call its initialize methods in each activity that you want to test
 * Remember about proper ordering of calls in your onCreate() method
 */
public class DebugHelper {

    /**
     * Use this to decide wheter DebugHelper should run only when you are debugging
     */
    public static boolean WORK_ONLY_WHEN_DEBUGGING = true;

    /**
     * Use this method to initialize DebugHelper in particular activity
     * @param activity pass your activity in which DebugHelper should work
     * @param strictModeOn true if you want to use StrictMode
     * @param leakCanaryOn true if you want to use LeakCanary
     * @return array of booleans containing
     * [0] - was initialization attempted, [1] - is StrictMode enabled, [2] - is LeakCanary enabled
     */
    public static boolean[] initializeDebugHelpers(Activity activity, boolean strictModeOn, boolean leakCanaryOn) {
        boolean shouldBeInitialized = true;

        if (WORK_ONLY_WHEN_DEBUGGING) {
            if (android.util.Config.DEBUG) {
                shouldBeInitialized = true;
            }
            else {
                shouldBeInitialized = false;
            }
        }

        if (shouldBeInitialized) {
            if (strictModeOn) {
                initializeStrictMode();
            }
            if (leakCanaryOn) {
                initializeLeakCanary(activity.getApplication());
            }
        }

        boolean[] results = {shouldBeInitialized, strictModeOn, leakCanaryOn};
        return results;
    }

    /**
     * Initialize StrictMode component
     */
    private static void initializeStrictMode() {
        StrictMode.setThreadPolicy(new StrictMode.ThreadPolicy.Builder()
                .detectDiskReads()
                .detectDiskWrites()
                .detectNetwork()   // or .detectAll() for all detectable problems
                .penaltyLog()
                .build());
        StrictMode.setVmPolicy(new StrictMode.VmPolicy.Builder()
                .detectLeakedSqlLiteObjects()
                .detectLeakedClosableObjects()
                .penaltyLog()
                .penaltyDeath()
                .build());
    }

    /**
     * Initialize LeakCanary component
     * @param application - application in which you want to use LeakCanary
     */
    private static void initializeLeakCanary(Application application) {
            LeakCanary.install(application);
    }
}