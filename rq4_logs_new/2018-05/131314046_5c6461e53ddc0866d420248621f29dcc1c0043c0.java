package com.example.caucse.baseui;

import android.content.Context;
import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;

import com.example.caucse.baseui.database.DatabaseHelper;

public class DBHandler {
    private DatabaseHelper helper;
    private SQLiteDatabase mDatabase;

    private DBHandler(Context context){
        this.helper = new DatabaseHelper(context);
        this.mDatabase = helper.getReadableDatabase();
    }

    public static DBHandler open(Context context) throws SQLException{
        DBHandler handler = new DBHandler(context);
        return handler;
    }

    public void close() {helper.close(); }

    public Cursor select(int ID)throws SQLException{
        //name 질의
        Cursor cursor = mDatabase.query(true, "Beerinfo",
                new String[]{"ID","Name","Country","Flavor","Kind","IBU","Alcohol","kcal"}, "ID"
        + "=" +  ID, null, null, null, null, null);
        if(cursor != null){
            cursor.moveToFirst();
        }
        return cursor;
    }

}