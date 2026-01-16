package com.example.cst2335.lyrics_ovh;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import java.util.ArrayList;

/**
 * Helps to communicate with SQLiteDatabase
 */
public class LyricsDatabase extends SQLiteOpenHelper {

    /**
     * Table name
     */
    String favouritesTable = "favourites";
    /**
     * Primary key
     */
    String recordId = "id";
    /**
     * Column name for song title
     */
    String songTitle = "songTitle";
    /**
     * Column name for artist or band name
     */
    String artistName = "artistName";
    /**
     * Column name for song lyrics
     */
    String lyrics = "lyrics";

    /**
     * @param context Context of activity or fragment
     * */
    public LyricsDatabase(Context context) {
        super(context, "LyricsDatabase", null, 1);
    }

    /**
     * Creates the database on the first run
     * */
    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(
                "CREATE TABLE " + favouritesTable + "(" + recordId + " INTEGER PRIMARY KEY AUTOINCREMENT, "
                        + songTitle + " text," +
                        artistName + " text, " + lyrics + " text)"
        );
    }

    /**
     * drops and creates new database on update
     * */
    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        String dropQuery = "DROP TABLE " + favouritesTable;
        db.execSQL(dropQuery);
        onCreate(db);
    }

    /**
     * @return List of saved records for songs with lyrics
     * */
    public ArrayList<SongDetails> getAllFavourites() {
        ArrayList<SongDetails> songDetails = new ArrayList<>();
        //get database object
        SQLiteDatabase database = this.getReadableDatabase();
        //perform query
        Cursor cursor = database.query(favouritesTable,
                new String[]{recordId, songTitle, artistName, lyrics},
                null, null,
                null, null, null, null);
        //check if the data returned
        if (cursor.moveToFirst()) {
            do {
                //read the retrieved data and put it in the list
                SongDetails songDetail = new SongDetails();
                songDetail.setRecordId(cursor.getString(0));
                songDetail.setTitle(cursor.getString(1));
                songDetail.setArtist(cursor.getString(2));
                songDetail.setLyrics(cursor.getString(3));
                songDetails.add(songDetail);
            } while (cursor.moveToNext());
        }
        //close cursor and database after performing an operation
        cursor.close();
        database.close();
        return songDetails;
    }

    /**
     * Stores the new record of the song
     * @param songDetails An object containing all the details containing lyrics
     * */
    public void addToFavourites(SongDetails songDetails) {
        SQLiteDatabase db = this.getWritableDatabase();
        ContentValues values = new ContentValues();
        values.put(songTitle, songDetails.getTitle());
        values.put(artistName, songDetails.getArtist());
        values.put(lyrics, songDetails.getLyrics());
        //perform insert operation
        db.insert(favouritesTable, null, values);
        db.close();
    }

    /**
     * Checks if the data already exists or not containing artist and song title
     * @param artist name of an artist or band
     * @param title title of the song
     * */
    public String isFavouriteExists(String artist, String title) {
        boolean isFavouriteExists;
        SQLiteDatabase database = this.getReadableDatabase();
        Cursor cursor = database.query(favouritesTable,
                new String[]{recordId},
                artistName + "=? AND " + songTitle + "=?", new String[]{artist, title},
                null, null, null, null);
        isFavouriteExists = cursor.moveToFirst();
        String songId = "0";
        if (isFavouriteExists) {
            songId = cursor.getString(0);
        }
        cursor.close();
        database.close();
        return songId;
    }

    /**
     * Removes the favourite from the database
     * @param index id of the record stored in table
     * */
    public void deleteFavourite(String index) {
        SQLiteDatabase db = this.getWritableDatabase();
        db.delete(favouritesTable, recordId + " = ?",
                new String[]{index});
        db.close();
    }
}