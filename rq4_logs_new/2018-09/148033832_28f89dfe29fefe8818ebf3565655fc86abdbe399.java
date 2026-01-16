package com.mathias.android.notes;


import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.provider.BaseColumns;

import java.util.ArrayList;
import java.util.List;

final class DBManager {

    private DBManager() {
    }

    static long saveNote(Context context, Note note) {
        DBHelperNotes dbHelper = new DBHelperNotes(context);
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        ContentValues values = new ContentValues();
        values.put(NoteEntry.COLUMN_NAME_TITLE, note.getTitle());
        values.put(NoteEntry.COLUMN_NAME_TEXT, note.getText());
        values.put(NoteEntry.COLUMN_NAME_TIME, note.getTimestamp());
        values.put(NoteEntry.COLUMN_NAME_POS, note.getPos());
        return db.insert(NoteEntry.TABLE_NAME, null, values);
    }

    static List<Note> getNotes(Context context) {
        DBHelperNotes dbHelper = new DBHelperNotes(context);
        SQLiteDatabase db = dbHelper.getReadableDatabase();
        String[] projection = {
                NoteEntry._ID,
                NoteEntry.COLUMN_NAME_TITLE,
                NoteEntry.COLUMN_NAME_TEXT,
                NoteEntry.COLUMN_NAME_TIME,
                NoteEntry.COLUMN_NAME_POS
        };
        String selection = NoteEntry.COLUMN_NAME_TITLE + " LIKE ?";
        String selectionArgs[] = {"%"};
        String sortOrder = NoteEntry.COLUMN_NAME_POS + " DESC";
        Cursor c = db.query(
                NoteEntry.TABLE_NAME,    // The table to query
                projection,              // The columns to return
                selection,               // The columns for the WHERE clause
                selectionArgs,           // The values for the WHERE clause
                null,            // don't group the rows
                null,             // don't filter by row groups
                sortOrder                // The sort order
        );
        List<Note> notes = new ArrayList<>();
        if (c != null) {
            if (c.moveToFirst()) {
                do {
                    long id = c.getLong(c.getColumnIndexOrThrow(NoteEntry._ID));
                    int pos = c.getInt(c.getColumnIndexOrThrow(NoteEntry.COLUMN_NAME_POS));
                    String title = c.getString(c.getColumnIndexOrThrow(NoteEntry.COLUMN_NAME_TITLE));
                    String text = c.getString(c.getColumnIndexOrThrow(NoteEntry.COLUMN_NAME_TEXT));
                    String time = c.getString(c.getColumnIndexOrThrow(NoteEntry.COLUMN_NAME_TIME));
                    notes.add(new Note(id, pos, title, text, time));
                } while (c.moveToNext());
            }
            c.close();
        }
        return notes;
    }

    static void deleteNote(Context context, long id) {
        DBHelperNotes dbHelper = new DBHelperNotes(context);
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        String selection = NoteEntry._ID + " = ?";
        String[] selectionArgs = {String.valueOf(id)};
        db.delete(NoteEntry.TABLE_NAME, selection, selectionArgs);
    }

    static void deleteAllNotes(Context context) {
        DBHelperNotes dbHelper = new DBHelperNotes(context);
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        db.delete(NoteEntry.TABLE_NAME, null, null);
    }

    static void setNotePos(Context context, long id, int pos) {
        DBHelperNotes dbHelper = new DBHelperNotes(context);
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        String query = "UPDATE " + NoteEntry.TABLE_NAME +
                " SET " + NoteEntry.COLUMN_NAME_POS + " = " + pos +
                " WHERE " + NoteEntry._ID + " = " + id;
        db.execSQL(query);
    }

    private static final class DBHelperNotes extends SQLiteOpenHelper {
        // METADATA
        private static final int DATABASE_VERSION = 1;
        private static final String DATABASE_NAME = "Notes.db";
        // QUERIES
        private static final String TEXT_TYPE = " TEXT";
        private static final String INTEGER_TYPE = " INTEGER";
        private static final String COMMA_SEP = ",";
        private static final String SQL_DELETE_ENTRIES = "DROP TABLE IF EXISTS " + NoteEntry.TABLE_NAME;
        private static final String SQL_CREATE_ENTRIES =
                "CREATE TABLE " + NoteEntry.TABLE_NAME + " (" +
                        NoteEntry._ID + " INTEGER PRIMARY KEY," +
                        NoteEntry.COLUMN_NAME_TITLE + TEXT_TYPE + COMMA_SEP +
                        NoteEntry.COLUMN_NAME_TEXT + TEXT_TYPE + COMMA_SEP +
                        NoteEntry.COLUMN_NAME_POS + INTEGER_TYPE + COMMA_SEP +
                        NoteEntry.COLUMN_NAME_TIME + TEXT_TYPE + " )";

        DBHelperNotes(Context context) {
            super(context, DATABASE_NAME, null, DATABASE_VERSION);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(SQL_CREATE_ENTRIES);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL(SQL_DELETE_ENTRIES);
            onCreate(db);
        }

        @Override
        public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            onUpgrade(db, oldVersion, newVersion);
        }
    }

    private static class NoteEntry implements BaseColumns {
        private static final String TABLE_NAME = "notes";
        private static final String COLUMN_NAME_TITLE = "title";
        private static final String COLUMN_NAME_TEXT = "text";
        private static final String COLUMN_NAME_TIME = "time";
        private static final String COLUMN_NAME_POS = "pos";
    }
}


