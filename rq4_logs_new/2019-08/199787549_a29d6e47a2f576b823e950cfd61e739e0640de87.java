package com.example.myapplication;

import android.app.Application;

import com.example.myapplication.di.AppComponent;
import com.example.myapplication.di.AppModule;
import com.example.myapplication.di.DaggerAppComponent;

public class App extends Application {
    static private App instance;

    private AppComponent appComponent;

    public static App getInstance() {
        return instance;
    }

    @Override
    public void onCreate() {
        super.onCreate();
        instance = this;

        appComponent = DaggerAppComponent.builder()
                .appModule(new AppModule(this))
                .build();
    }

    public AppComponent getAppComponent() {
        return appComponent;
    }
}