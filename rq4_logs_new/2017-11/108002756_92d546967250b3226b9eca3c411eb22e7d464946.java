package com.test.murphy.weatherapp;

import android.app.Application;
import android.content.Context;

/**
 * Created by wsmurphy on 11/13/17.
 */

public class WeatherApp extends Application {

    private static Context mContext;
    private static WeatherApp mInstance;

    public WeatherApp() {
    }

    public static WeatherApp getInstance() {
        return mInstance;
    }

    @Override
    public void onCreate() {
        super.onCreate();

        mContext = getApplicationContext();
    }

    public static Context getContext() { return mContext; }
}