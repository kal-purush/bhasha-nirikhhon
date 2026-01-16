package com.example.finalproject.utilities;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Shower on 2016/12/24 0024.
 */

public class DBHelper extends SQLiteOpenHelper {
    private static final String DB_NAME = "UserDataDB";
    private static final String TABLE_NAME = "User";
    private static final int DB_VERSION = 1;

    public static final List<UserData> userDataHistory = new ArrayList<>();

    public DBHelper(Context context, String name, SQLiteDatabase.CursorFactory factory, int version) {
        super(context, name, factory, version);
    }

    public DBHelper(Context context) {
        this(context, DB_NAME, null, DB_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        String CREATE_TABLE = "CREATE TABLE if not exists "
                + TABLE_NAME
                + " (date TEXT PRIMARY KEY NOT NULL, total_working_time INTEGER, step_count INTEGER)";
        db.execSQL(CREATE_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }

    public boolean insert(UserData userData) {
        SQLiteDatabase db = getWritableDatabase();
        ContentValues cv = new ContentValues();
        cv.put("date", userData.getDate());
        cv.put("total_working_time", userData.totalWorkingTime);
        cv.put("step_count", userData.stepCount);
        long result = db.insert(TABLE_NAME, null, cv);
        return result != -1;
    }

    public void update(UserData userData) {
        SQLiteDatabase db = getWritableDatabase();
        ContentValues cv = new ContentValues();
        cv.put("total_working_time", userData.totalWorkingTime);
        cv.put("step_count", userData.stepCount);
        db.update(TABLE_NAME, cv, "date=?", new String[]{userData.getDate()});
    }

    public void delete(UserData userData) {
        SQLiteDatabase db = getWritableDatabase();
        db.delete(TABLE_NAME, "date=?", new String[]{userData.getDate()});
    }

    public List<UserData> getHistory() throws ParseException {
        SQLiteDatabase db = getWritableDatabase();
        userDataHistory.clear();
        Cursor cursor = db.query(TABLE_NAME, null, null, null, null, null, null);
        if (cursor.moveToFirst()) {
            do {
                userDataHistory.add(new UserData(cursor.getString(0), cursor.getInt(1), cursor.getInt(2)));
            } while (cursor.moveToNext());
        }
        return userDataHistory;
    }
}