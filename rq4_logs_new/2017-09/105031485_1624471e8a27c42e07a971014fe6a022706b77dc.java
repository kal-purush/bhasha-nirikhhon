package com.banan;

import android.app.Application;
import android.content.Context;

import com.banan.model.entities.MyObjectBox;

import io.objectbox.BoxStore;

public class App extends Application {

    private static Context context;
    private BoxStore boxStore;

    @Override
    public void onCreate() {
        super.onCreate();
        context = getApplicationContext();

        // This is the minimal setup required on Android
        boxStore = MyObjectBox.builder().androidContext(App.this).build();
    }

    public BoxStore getBoxStore() {
        return boxStore;
    }

    public static Context getContext() {
        return context;
    }
}