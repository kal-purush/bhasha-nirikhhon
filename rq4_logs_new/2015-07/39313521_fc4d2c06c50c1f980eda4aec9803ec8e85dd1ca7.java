package com.example.emp00037872.recyclerproject;

import android.app.Application;
import android.content.Context;

/**
 * Created by Вадим on 19.07.2015.
 */
public class App extends Application {
    public static Context context;

    public void OnCreate() {
        super.onCreate();
        this.context = getApplicationContext();
    }
}