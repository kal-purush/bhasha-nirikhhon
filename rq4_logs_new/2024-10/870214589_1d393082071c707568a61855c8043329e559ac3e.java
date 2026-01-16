package com.sanika.beachapplication.api;

import android.annotation.SuppressLint;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

import com.sanika.beachapplication.model.Beach;

import java.util.ArrayList;
import java.util.List;

public class BeachDatabaseManager {
    private DatabaseHelper dbHelper;

    public BeachDatabaseManager(Context context) {
        dbHelper = new DatabaseHelper(context);
    }

    public void insertBeach(Beach beach) {
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        ContentValues values = new ContentValues();
        values.put(DatabaseHelper.COLUMN_NAME, beach.getName());
        values.put(DatabaseHelper.COLUMN_LOCATION, beach.getLocation());
        values.put(DatabaseHelper.COLUMN_IMAGE_URL, beach.getImageUrl());

        db.insert(DatabaseHelper.TABLE_BEACHES, null, values);
        db.close();
    }
    @SuppressLint("Range")
    public List<Beach> getAllBeaches() {
        List<Beach> beachList = new ArrayList<>();
        SQLiteDatabase db = dbHelper.getReadableDatabase();
        Cursor cursor = db.query(DatabaseHelper.TABLE_BEACHES, null, null, null, null, null, null);

        if (cursor.moveToFirst()) {
            do {
                Beach beach = new Beach();
                beach.setId(cursor.getInt(cursor.getColumnIndex(DatabaseHelper.COLUMN_ID)));
                beach.setName(cursor.getString(cursor.getColumnIndex(DatabaseHelper.COLUMN_NAME)));
                beach.setLocation(cursor.getString(cursor.getColumnIndex(DatabaseHelper.COLUMN_LOCATION)));
                beach.setImageUrl(cursor.getString(cursor.getColumnIndex(DatabaseHelper.COLUMN_IMAGE_URL)));
                beachList.add(beach);
            } while (cursor.moveToNext());
        }
        cursor.close();
        db.close();
        return beachList;
    }
}