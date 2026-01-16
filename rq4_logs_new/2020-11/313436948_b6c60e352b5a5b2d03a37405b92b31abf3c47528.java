package com.example.todolist;

import android.app.Application;

import com.example.todolist.data.AppDatabase;
import com.example.todolist.data.NoteDao;

import androidx.room.Room;
import androidx.room.RoomDatabase;

public class App extends Application {

    private NoteDao noteDao;

    private static App instance;
    private AppDatabase database;

    public static App getInstance() {
        return instance;
    }

    @Override
    public void onCreate() {
        super.onCreate();

        instance = this;

        database = Room.databaseBuilder(getApplicationContext(),
                AppDatabase.class, "app-db-name")
                .allowMainThreadQueries()
                .build();

        noteDao = database.noteDao();
    }

    public AppDatabase getDatabase() {
        return database;
    }

    public void setDatabase(AppDatabase database) {
        this.database = database;
    }

    public NoteDao getNoteDao() {
        return noteDao;
    }

    public void setNoteDao(NoteDao noteDao) {
        this.noteDao = noteDao;
    }
}