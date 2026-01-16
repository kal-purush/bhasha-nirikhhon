package ru.mirea.sysoeva.mireaproject.DataTask8pr;

import android.app.Application;

import androidx.room.Room;

public class App extends Application {
    public static App instance;
    private AppDatebase database;

    @Override
    public void onCreate() {
        super.onCreate();
        instance = this;
        database = Room.databaseBuilder(this, AppDatebase.class, "database")
                .allowMainThreadQueries()
                .fallbackToDestructiveMigration()
                .build();
    }

    public static App getInstance() {
        return instance;
    }

    public AppDatebase getDatabase() {
        return database;
    }
}