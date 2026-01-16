package com.example.projectpmob;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import androidx.annotation.Nullable;

public class DatabaseHelper extends SQLiteOpenHelper {

    public DatabaseHelper(@Nullable Context context) {
        super(context, "foodfast.db", null, 1);
    }

    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {
        sqLiteDatabase.execSQL("CREATE TABLE restaurant(id integer PRIMARY KEY AUTOINCREMENT, nama text, alamat text, rating integer)");
        sqLiteDatabase.execSQL("INSERT INTO restaurant(id, nama, alamat, rating) VALUES(1, 'RM.Padang Jaya', 'JL.Bandung 12', 8)");
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int i, int i1) {

    }

    public void perbarui(SQLiteDatabase db){
        db.execSQL("DROP TABLE IF EXISTS restaurant");
        onCreate(db);
    }
}