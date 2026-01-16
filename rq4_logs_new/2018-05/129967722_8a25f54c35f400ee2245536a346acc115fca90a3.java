package com.example.juanalvaropupo.interaptskillsapp;

import android.app.Application;
import android.arch.persistence.room.Room;

public class StudentApplication extends Application {

    private StudentsListDatabase database;
    public static String DATABASE_NAME = "student_list_database";

    @Override
    public void onCreate() {
        super.onCreate();

        database = Room.databaseBuilder(getApplicationContext(), StudentsListDatabase.class, DATABASE_NAME)
                .allowMainThreadQueries()
                .build();
    }

    public StudentsListDatabase getDatabase() {
        return database;
    }
}