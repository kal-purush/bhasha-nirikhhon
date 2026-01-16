package com.example.imperial_to_si.database;

import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

import static com.example.imperial_to_si.database.DatabaseConstraints.COLUMN_FACTOR;
import static com.example.imperial_to_si.database.DatabaseConstraints.COLUMN_ID;
import static com.example.imperial_to_si.database.DatabaseConstraints.COLUMN_NAME;
import static com.example.imperial_to_si.database.DatabaseConstraints.COLUMN_UNIT_SI;
import static com.example.imperial_to_si.database.DatabaseConstraints.DATABASE_NAME;
import static com.example.imperial_to_si.database.DatabaseConstraints.TABLE_NAME;

public class DbHandler extends SQLiteOpenHelper {

    private static final int DB_VERSION = 1;

    public DbHandler(Context context) {

        super(context, DATABASE_NAME, null, DB_VERSION);

        Log.d("Database operations", "Database created.");
    }

    @Override
    public void onCreate(SQLiteDatabase database) {

        database.execSQL("CREATE TABLE " + TABLE_NAME + " (" +
                COLUMN_ID + " INTEGER PRIMARY KEY, " +
                COLUMN_NAME + " VARCHAR(20), " +
                COLUMN_FACTOR + " REAL, " +
                COLUMN_UNIT_SI + " VARCHAR(5))");
        database.execSQL("INSERT INTO " + TABLE_NAME + "(" + COLUMN_ID + ", " + COLUMN_NAME + ", " + COLUMN_FACTOR + ", " + COLUMN_UNIT_SI + ") VALUES " +
                "(1, \"cal\", 0.0254, \"m\")," +
                "(2, \"stopa\", 0.3048, \"m\")," +
                "(3, \"jard\", 0.9144, \"m\")," +
                "(4, \"mila\", 1609.3, \"m\")," +
                "(5, \"liga\", 4827.9, \"m\")," +
                "(6, \"akr\", 4046.9, \"m2\")," +
                "(7, \"pinta\", 0.0005683, \"m3\")," +
                "(8, \"galon\", 0.0045461, \"m3\")," +
                "(9, \"uncja\", 0.02835, \"kg\")," +
                "(10, \"funt\", 0.4536, \"kg\")"
        );
        Log.d("Database operations", "Database successfully populated.");
    }

    @Override
    public void onUpgrade(SQLiteDatabase database, int oldVersion, int newVersion) {

        database.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
        onCreate(database);
    }

    public Cursor getAllUnits(SQLiteDatabase database) {

        String[] projections = {COLUMN_ID, COLUMN_NAME, COLUMN_FACTOR, COLUMN_UNIT_SI};
        return database.query(TABLE_NAME, projections, null, null, null, null, COLUMN_ID);
    }
}