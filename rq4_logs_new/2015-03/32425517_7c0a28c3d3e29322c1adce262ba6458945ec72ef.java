package com.example.tripplanner;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

import java.util.ArrayList;

public class SQLHelper extends SQLiteOpenHelper {

	public static final String DATABASE_NAME = "trip.db";
	public static final int DATABASE_VERSION = 1;
	public static final String TBL_TRIP = "trip";
	public static final String TBL_ALERT = "alert";
	public static final String TBL_PACK = "pack";
	public static final String TBL_TODO = "todo";
	public static final String TBL_PLACE = "place";
	public static final String KEY_NAME = "name";
	public static final String TRIP_ID = "trip_id";

	public static final String[] TABLES = { "trip", "alert", "pack", "todo",
			"place" };
	public static final String KEY_ID = "id integer primary key autoincrement";

	/************************* BUILD DATABASE *********************************/
	/**
	 * public static final String CREATE_TABLE = "CREATE TABLE trip (" + KEY_ID
	 * + "," + KEY_NAME + " text,;";
	 */

	public static final String CREATE_TBL_TRIP = "CREATE TABLE " + TBL_TRIP
			+ "(" + KEY_ID + "," + KEY_NAME + " text,;";

	public static final String CREATE_TBL_PACK = "CREATE TABLE" + TBL_PACK
			+ "(" + KEY_ID + "," + TRIP_ID + " INTEGER," + KEY_NAME + " text,;";

	public static final String CREATE_TBL_TODO = "CREATE TABLE" + TBL_TODO
			+ "(" + KEY_ID + "," + TRIP_ID + " INTEGER," + KEY_NAME + " text,;";

	// Array of create table SQL statements
	public static final String[] SQL_CREATE = { CREATE_TBL_TRIP,
			CREATE_TBL_PACK, CREATE_TBL_TODO };

	// Method to delete from a table

	/* Method to add a new trip
	public void addTrip(Trip item) {
		SQLiteDatabase db = this.getWritableDatabase();
		db.close();
	}*/

	// Method to insert into a table

	// Method to update a table

	private ContentValues values;
	private Cursor cursor;

	public SQLHelper(Context context) {
		super(context, DATABASE_NAME, null, DATABASE_VERSION);
	}

	// called to create table
	@Override
	public void onCreate(SQLiteDatabase db) {

		for (String s : SQL_CREATE) {
			String sql = s;
			Log.d("SQLiteDemo", "onCreate: " + sql);
			db.execSQL(sql);
		}
	}

	// called when database version mismatch
	@Override
	public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

		if (oldVersion >= newVersion)
			return;

		Log.d("SQLiteDemo", "onUpgrade: Version = " + newVersion);
		for (int i = 0; i < TABLES.length; i++) {
			db.execSQL("DROP TABLE IF EXISTS " + TABLES[i]);
			onCreate(db);
		}

	}
}