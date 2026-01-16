package com.example.administrator.smallvault.db;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.provider.BaseColumns;
import android.util.Log;

public class DatabaseHelper extends SQLiteOpenHelper {

    public static final String DATABASE_NAME = "smallvault.db";
    public static final int DATABASE_VERSION = 1;
    public static final String TABLE_FIRST_NAME = "first";
    public static final String TABLE_SECOND_NAME = "second";
    public static final String SQL_CREATE_TABLE_FIRST = "CREATE TABLE " +TABLE_FIRST_NAME +"("
            + BaseColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT,"
            + "table_name" +" VARCHAR(50) default 'first',"
            + "name" + " VARCHAR(50),"
            + "detail" + " TEXT"
            + ");" ;
    public static final String SQL_CREATE_TABLE_SECOND = "CREATE TABLE "+TABLE_SECOND_NAME+" ("
            + BaseColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT,"
            + "table_name" +" VARCHAR(50) default 'second',"
            + "name" + " VARCHAR(50),"
            + "detail" + " TEXT"
            + ");" ;

    public DatabaseHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        Log.e("smallvault", "create table: " + SQL_CREATE_TABLE_FIRST);
        db.execSQL(SQL_CREATE_TABLE_FIRST);
        db.execSQL(SQL_CREATE_TABLE_SECOND);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS first");
        db.execSQL("DROP TABLE IF EXISTS second");
        onCreate(db);
    }
}