package com.example.romanm.recyclerpagination;

import android.app.Application;

import com.example.romanm.recyclerpagination.di.component.AppComponent;
import com.example.romanm.recyclerpagination.di.component.DaggerAppComponent;
import com.example.romanm.recyclerpagination.di.modules.ContextModule;

/**
 * Created by RomanM on 29.10.2017.
 */

public class App extends Application {

    private static AppComponent appComponent;


    @Override
    public void onCreate() {
        super.onCreate();

        appComponent = DaggerAppComponent.builder()
                .contextModule(new ContextModule(this))
                .build();
    }

    public static AppComponent getAppComponent() {
        return appComponent;
    }
}