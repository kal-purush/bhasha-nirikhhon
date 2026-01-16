package com.clubjevin.festivus.data;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kevin on 12/1/16.
 */

public class GrievancesDAO {
    private final GrievancesDbHelper dbHelper;
    private final SQLiteDatabase db;

    public GrievancesDAO(Context context) {
        dbHelper = new GrievancesDbHelper(context);
        db = dbHelper.getWritableDatabase();
    }

    public void close() {
        dbHelper = null;

        if(db != null) {
            db.close();
        }
        db = null;
    }

    public void insert(Grievance grievance) {
        ContentValues values = new ContentValues();
        values.put(GrievancesDbHelper.ColumnNames.TIMESTAMP, grievance.getTimestamp());
        values.put(GrievancesDbHelper.ColumnNames.GRIEVANCE, grievance.getContent());

        synchronized(db) {
            db.insert(dbHelper.TABLE_NAME, null, values);
        }
    }

    public List<Grievance> read() {
        String[] columnNames = {
                GrievancesDbHelper.ColumnNames.TIMESTAMP,
                GrievancesDbHelper.ColumnNames.GRIEVANCE
        };

        Cursor cursor = null;
        synchronized(db) {
            cursor = db.query(GrievancesDbHelper.TABLE_NAME, columnNames, null, null, null, null, null);
        }

        List<Grievance> result = new ArrayList<>();

        boolean rowsRemain = cursor.moveToFirst();
        while(rowsRemain) {
            result.add(new Grievance(cursor.getLong(0), cursor.getString(1)));
            rowsRemain = cursor.moveToNext();
        }

        return result;
    }
}