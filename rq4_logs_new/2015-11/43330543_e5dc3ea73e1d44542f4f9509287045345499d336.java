package com.leechangu.sweettask;

import android.app.Application;

import com.parse.Parse;

/**
 * Created by CharlesGao on 15-11-16.
 */
public class App extends Application {

    /**
     * Called when the application is starting, before any activity, service,
     * or receiver objects (excluding content providers) have been created.
     * Implementations should be as quick as possible (for example using
     * lazy initialization of state) since the time spent in this function
     * directly impacts the performance of starting the first activity,
     * service, or receiver in a process.
     * If you override this method, be sure to call super.onCreate().
     */
    @Override
    public void onCreate() {
        super.onCreate();

        // Enable Local Datastore.
        Parse.enableLocalDatastore(this);
        Parse.initialize(this, "RkIHvpR9hEdejBizCivkmywOflupRH49ozgFHROt", "ZqTOMc3F7JucJzOzJc0rt3st4Iffy2ij6njXc0QX");

    }
}