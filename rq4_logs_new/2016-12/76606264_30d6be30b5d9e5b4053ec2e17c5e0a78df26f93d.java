package com.example.shafy.whatsthere.Data;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by mina essam on 16-Dec-16.
 */
public class NewsDbHelper extends SQLiteOpenHelper {
    public static final int  DATABASE_VERSION=1;
    public static final String DATABASE_NAME="NewsDatabase.db";
    public NewsDbHelper(Context context){
        super(context,DATABASE_NAME,null,DATABASE_VERSION);
    }
    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(WhatsThereContract.CREATE_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL(WhatsThereContract.DELETE_TABLE);
        onCreate(db);

    }
}