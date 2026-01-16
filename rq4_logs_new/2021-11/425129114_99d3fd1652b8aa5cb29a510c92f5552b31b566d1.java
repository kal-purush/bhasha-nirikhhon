package com.example.myapplication.db;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import androidx.annotation.Nullable;

public class DbHelper extends SQLiteOpenHelper {

    private static final int DATABASE_VERSION = 2;
    private static final String DATABASE_NOMBRE = "task.db";
    public static final String TABLE_TASK = "t_task" ;


    public DbHelper(@Nullable Context context) {
        super(context, DATABASE_NOMBRE,null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {

        sqLiteDatabase.execSQL("CREATE TABLE " + TABLE_TASK + "(" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "idImagen INTEGER NOT NULL," +
                "nombre TEXT NOT NULL," +
                "descripcion TEXT NOT NULL," +
                "fecha TEXT NOT NULL," +
                "typeTask TEXT NOT NULL)");

    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int i, int i1) {
        sqLiteDatabase.execSQL("DROP TABLE " + TABLE_TASK);
        onCreate(sqLiteDatabase);
    }
}