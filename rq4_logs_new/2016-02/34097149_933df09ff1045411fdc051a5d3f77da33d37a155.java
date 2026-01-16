package com.spartacus.shotclockscorekeeper;

import com.crashlytics.android.Crashlytics;

import io.fabric.sdk.android.Fabric;

public class Application extends android.app.Application {

    @Override
    public void onCreate() {
        super.onCreate();
        Fabric.with(this, new Crashlytics());
    }
}