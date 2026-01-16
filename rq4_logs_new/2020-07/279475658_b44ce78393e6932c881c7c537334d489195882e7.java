package com.example.cst2335.soccer;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import java.util.ArrayList;

/**
 * Used for performing database operations like insert delete and read
 * */
public class SoccerDatabase extends SQLiteOpenHelper {

    private static final String SOCCER_FAV_TABLE = "soccer_favourite_matches";
    private static final String TITLE = "title";
    private static final String COMPETITION = "competition";
    private static final String DATE = "date";
    private static final String SIDE1NAME = "side_one";
    private static final String SIDE2NAME = "side_two";
    private static final String VIDEOURL = "videoURL";
    private final String createQuery = "CREATE TABLE " + SOCCER_FAV_TABLE + " ("
            + TITLE + " TEXT, "
            + COMPETITION + " TEXT, "
            + DATE + " TEXT, "
            + SIDE1NAME + " TEXT, "
            + SIDE2NAME + " TEXT, "
            + VIDEOURL + " TEXT"
            + ")";
    private final String dropQuery = "DROP TABLE " + SOCCER_FAV_TABLE;

    SoccerDatabase(Context context) {
        super(context, "soccer_database", null, 1);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(createQuery);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL(dropQuery);
        onCreate(db);
    }

    /**
     * Inserts the record to the database
     * @param soccerMatch Match details to save in the database
     * */
    void insert(SoccerMatch soccerMatch) {
        ContentValues contentValues = new ContentValues();
        contentValues.put(TITLE, soccerMatch.getTitle());
        contentValues.put(COMPETITION, soccerMatch.getCompetition());
        contentValues.put(DATE, soccerMatch.getDate());
        contentValues.put(SIDE1NAME, soccerMatch.getSide1Name());
        contentValues.put(SIDE2NAME, soccerMatch.getSide2Name());
        contentValues.put(VIDEOURL, soccerMatch.getVideoURL());

        SQLiteDatabase database = getWritableDatabase();
        database.insert(SOCCER_FAV_TABLE, null, contentValues);
        database.close();
    }

    /**
     * Reads all the records from the database and returns the list of fetched record
     * @return SoccerMatch list form  the database
     * */
    ArrayList<SoccerMatch> selectAll() {
        ArrayList<SoccerMatch> soccerMatches = new ArrayList<>();
        SQLiteDatabase database = getReadableDatabase();
        Cursor cursor = database.query(SOCCER_FAV_TABLE,
                new String[]{
                        TITLE,
                        COMPETITION,
                        DATE,
                        SIDE1NAME,
                        SIDE2NAME,
                        VIDEOURL
                },
                null, null,
                null, null, null, null);
        if (cursor.moveToFirst()) {
            do {
                String title = cursor.getString(0);
                String competition = cursor.getString(1);
                String date = cursor.getString(2);
                String side1Name = cursor.getString(3);
                String side2Name = cursor.getString(4);
                String videoUrl = cursor.getString(5);
                SoccerMatch soccerMatch = new SoccerMatch();
                soccerMatch.setTitle(title);
                soccerMatch.setCompetition(competition);
                soccerMatch.setDate(date);
                soccerMatch.setSide1Name(side1Name);
                soccerMatch.setSide2Name(side2Name);
                soccerMatch.setVideoURL(videoUrl);
                soccerMatches.add(soccerMatch);
            } while (cursor.moveToNext());
        }
        return soccerMatches;
    }

    /**
     * Checks the match with same title exists or not
     * @param title title of the match
     * */
    boolean selectMatch(String title) {
        SQLiteDatabase database = getReadableDatabase();
        Cursor cursor = database.query(SOCCER_FAV_TABLE,
                new String[]{TITLE, COMPETITION, DATE},
                TITLE + "=?", new String[]{title},
                null, null, null, null);
        boolean matchFound = cursor.moveToFirst();
        cursor.close();
        database.close();
        return matchFound;
    }

    /**
     * Deletes the match from the database with matching title
     * @param title Title of the match
     * */
    void deleteByName(String title) {
        SQLiteDatabase database = getWritableDatabase();
        database.delete(SOCCER_FAV_TABLE, TITLE + " = ?",
                new String[]{title});
        database.close();
    }
}