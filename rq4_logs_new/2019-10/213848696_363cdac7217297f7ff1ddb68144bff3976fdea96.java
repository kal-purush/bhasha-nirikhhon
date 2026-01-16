package com.example.avd;

import android.app.Activity;

import java.lang.ref.WeakReference;

public class ActivityLifeManager {

    private static ActivityLifeManager sInstance = new ActivityLifeManager();

    private WeakReference<Activity> sCurrentActivityWeakRef;

    private ActivityLifeManager() {

    }

    public static ActivityLifeManager getInstance() {
        return sInstance;
    }

    public Activity getCurrentActivity() {
        Activity currentActivity = null;
        if (sCurrentActivityWeakRef != null) {
            currentActivity = sCurrentActivityWeakRef.get();
        }
        return currentActivity;
    }

    public void setCurrentActivity(Activity activity) {
        sCurrentActivityWeakRef = new WeakReference<>(activity);
    }

}