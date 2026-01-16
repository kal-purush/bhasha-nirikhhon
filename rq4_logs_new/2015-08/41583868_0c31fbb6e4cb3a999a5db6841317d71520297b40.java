package com.tictaeto.x_project.db;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by DEN on 30.08.2015.
 */
public class DBHelper extends SQLiteOpenHelper {

    public DBHelper(Context context) {
        super(context, "db", null, 1);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("create table chat(id integer primary key autoincrement,chat_contact_id, name text, photo_id integer)");
        db.execSQL("create table message(id integer primary key autoincrement, chat_id integer, contact_id integer, content text)");
        db.execSQL("create table contact(id integer primary key autoincrement, chat_contact_id integer, name text, )");
        db.execSQL("create table chat_contact(chat_id integer, contact_id integer)");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }
}