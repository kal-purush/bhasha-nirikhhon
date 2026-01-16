package org.bcit.campuscompass;

import android.annotation.SuppressLint;
import android.content.Context;
import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DatabaseHelper extends SQLiteOpenHelper {

    private final Context context;
    private static final String DATABASE_NAME = "mapdata.db";
    private static final int DATABASE_VERSION = 1;

    private static final String TABLE_NAME = "Rooms";
    private static final String COLUMN_ID = "room_id";
    private static final String COLUMN_NUMBER = "room_number";    private static final String COLUMN_BUILDING = "building_id";
    private static final String COLUMN_FLOOR = "floor_id";

    public DatabaseHelper(Context context) {
        super(context, DATABASE_NAME , null, DATABASE_VERSION);
        this.context = context;


    }

    @Override
    public void onCreate(SQLiteDatabase db) {
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }

    public void copyDatabase() throws IOException {
        InputStream inputStream = context.getAssets().open(DATABASE_NAME);
        String outFileName = context.getDatabasePath(DATABASE_NAME).getPath();
        OutputStream outputStream = Files.newOutputStream(Paths.get(outFileName));

        byte[] buffer = new byte[1024];
        int length;
        while ((length = inputStream.read(buffer)) > 0) {
            outputStream.write(buffer, 0, length);
        }

        outputStream.flush();
        outputStream.close();
        inputStream.close();
    }

    public SQLiteDatabase openDatabase() throws SQLException {
        String path = context.getDatabasePath(DATABASE_NAME).getPath();
        return SQLiteDatabase.openDatabase(path, null, SQLiteDatabase.OPEN_READWRITE);
    }

    //this function takes the name of the location to return its dimensions by accessing the existing database
    //name of the location takes the following form: [building number]_floor_[floor number}
    //Example: getLocationDimensions(SW1_floor_1)
    //for campus: [campus city]_campus
    //Example: getLocationDimensions(burnaby_campus)
    //returns dimensions in a double array, where each element is the dimensions in S,W,E,N order
    public double[][] getLocationDimensions(String location){

        double[][] dimensions = new double[4][4];

        //define columns want to retrieve
        String[] projection = {"degrees", "minutes", "seconds", "bearing"};

        //define selection criteria
        String selection = "name = ?";

        dimensions[0] = mapQuery(selection, projection, location, "S");
        dimensions[1] = mapQuery(selection, projection, location, "W");
        dimensions[2] = mapQuery(selection, projection, location, "N");
        dimensions[3] = mapQuery(selection, projection, location, "E");

        return dimensions;
    }

    public double[][] getRoomLocation(String room) {
        double[][] location = new double[2][3];

        //define columns want to retrieve
        String[] projection = {"degrees", "minutes", "seconds"};

        //define selection criteria
        String selection = "name = ?";

        //note: room dimensions do not require N,S,E,W so direction is left as ""
        location[0] = roomQuery(selection, projection, room, "LAT");
        location[1] = roomQuery(selection, projection, room, "LONG");

        return location;
    }

    private double[] mapQuery(String selection, String[] projection, String location, String direction) {
        SQLiteDatabase db = this.getReadableDatabase();
        double[] dimensions = null;
        String[] selectionArgs = {location + direction};

        //Execute the query
        Cursor cursor = db.query("maps", projection, selection, selectionArgs, null, null, null);

        //check if the cursor has data
        if(cursor != null && cursor.moveToFirst()) {
            @SuppressLint("Range") double degrees = cursor.getDouble(cursor.getColumnIndex("degrees"));
            @SuppressLint("Range") double minutes = cursor.getDouble(cursor.getColumnIndex("minutes"));
            @SuppressLint("Range") double seconds = cursor.getDouble(cursor.getColumnIndex("seconds"));
            @SuppressLint("Range") double bearing = cursor.getDouble(cursor.getColumnIndex("bearing"));
            dimensions = new double[]{degrees, minutes, seconds, bearing};

            cursor.close();
        }

        //close database connection
        db.close();

        return dimensions;
    }
    private double[] roomQuery(String selection, String[] projection, String location, String direction) {
        SQLiteDatabase db = this.getReadableDatabase();
        double[] dimensions = null;
        String[] selectionArgs = {location + direction};

        //Execute the query
        Cursor cursor = db.query("rooms", projection, selection, selectionArgs, null, null, null);

        //check if the cursor has data
        if(cursor != null && cursor.moveToFirst()) {
            @SuppressLint("Range") double degrees = cursor.getDouble(cursor.getColumnIndex("degrees"));
            @SuppressLint("Range") double minutes = cursor.getDouble(cursor.getColumnIndex("minutes"));
            @SuppressLint("Range") double seconds = cursor.getDouble(cursor.getColumnIndex("seconds"));
            dimensions = new double[] {degrees, minutes, seconds};

            cursor.close();
        }

        //close database connection
        db.close();

        return dimensions;
    }
}