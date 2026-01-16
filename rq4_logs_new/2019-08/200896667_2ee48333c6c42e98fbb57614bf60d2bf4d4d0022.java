package com.example.rockpaperscissors;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDatabase.CursorFactory;
import android.database.sqlite.SQLiteOpenHelper;

public class DbHelper extends SQLiteOpenHelper{
	

	public DbHelper(Context context, String name, CursorFactory factory,
			int version) {
		super(context, name, factory, version);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void onCreate(SQLiteDatabase db) {
		// TODO Auto-generated method stub
		String highScoreCreateQuery="create table highscore(name text,score integer,username text, FOREIGN KEY(username) REFERENCES userDetails(username))";
		String userDetailsCreatyeQuery = "create table userDetails(name text,username text PRIMARY KEY ,password text, classicwins integer DEFAULT 0)";
		db.execSQL(userDetailsCreatyeQuery);
		db.execSQL(highScoreCreateQuery);
	}

	@Override
	public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
		// TODO Auto-generated method stub
		String highScoreDropQuery="drop table highscore";
		String userDetailsDropQuery="drop table userDetails";
		db.execSQL(highScoreDropQuery);
		db.execSQL(userDetailsDropQuery);
	}

}