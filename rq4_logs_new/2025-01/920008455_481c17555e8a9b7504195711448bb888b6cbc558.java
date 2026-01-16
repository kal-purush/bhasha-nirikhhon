package com.cashin.nest.demo;

import android.app.Application;

import timber.log.Timber;
import timber.log.Timber.DebugTree;

public class DemoApplication extends Application {
    @Override
    public void onCreate() {
        super.onCreate();
        Timber.plant(new DebugTree());
    }
}