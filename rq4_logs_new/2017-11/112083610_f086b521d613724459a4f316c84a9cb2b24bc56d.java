package com.tline.android.app;

import android.app.Application;
import android.support.annotation.NonNull;

import com.tline.android.BuildConfig;
import com.tline.android.app.injection.AppComponent;
import com.tline.android.app.injection.AppModule;
import com.tline.android.app.injection.DaggerAppComponent;

import timber.log.Timber;

public final class TLineApp extends Application {
    private AppComponent mAppComponent;

    @Override
    public void onCreate() {
        super.onCreate();

        // Shows logs only in Debug Build type
        if (BuildConfig.DEBUG) {
            Timber.plant(new Timber.DebugTree());
        }

        mAppComponent = DaggerAppComponent.builder()
                .appModule(new AppModule(this))
                .build();
    }

    @NonNull
    public AppComponent getAppComponent() {
        return mAppComponent;
    }
}