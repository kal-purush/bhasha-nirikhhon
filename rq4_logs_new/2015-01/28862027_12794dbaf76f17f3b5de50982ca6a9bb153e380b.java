package com.example.cottageofsweets_lite;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

public class DataBase extends SQLiteOpenHelper {

	//	table Colors
	public static final String TABLE_NAME = "Colors";
	public static final String COLUMN_ID = "_id";
	public static final String COLUMN_PIC = "picture";
	public static final String COLUMN_RED = "red";
	public static final String COLUMN_GREEN = "green";
	public static final String COLUMN_BLUE = "blue";
	private static final String DATABASE_NAME = "COS";

	//	table Quiz
	public static final String TABLE2_NAME = "Quiz";
	public static final String COLUMN2_ID = "_id";
	public static final String COLUMN2_QUIZ = "quiz";
	public static final String COLUMN2_ANS = "ans";
	
	Context context;
	private Vector<getColor> color_list = new Vector<getColor>();
	private Vector<getQuiz> quiz_list = new Vector<getQuiz>();

	public DataBase(Context ctx) {
		super(ctx, DATABASE_NAME, null, 1);
		context = ctx;
	}

	@Override
	public void onCreate(SQLiteDatabase db) {
		// Create table colors and quiz.
		db.execSQL("CREATE TABLE " + TABLE_NAME + "(" + COLUMN_ID
				+ " INTEGER PRIMARY KEY , " + COLUMN_PIC + " TEXT ,"
				+ COLUMN_RED + " INTEGER ," + COLUMN_GREEN + " INTEGER ,"
				+ COLUMN_BLUE + " INTEGER" + ");");

		Log.d("CREATE TABLE", "Create Color Table Successfully.");
		
		db.execSQL("CREATE TABLE " + TABLE2_NAME + "(" + COLUMN2_ID
	            + " INTEGER PRIMARY KEY , "
	            + COLUMN2_QUIZ+ " TEXT ,"
	            + COLUMN2_ANS+ " TEXT "+");");
		
		Log.i("CREATE TABLE","Create Quiz Table Successfully.");
	}

	@Override
	public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
		db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
		db.execSQL("DROP TABLE IF EXISTS " + TABLE2_NAME);
		onCreate(db);

	}
	
	//	import the color data(from file .csv) into database.
	public void createTableColor(){
		SQLiteDatabase db = this.getWritableDatabase();
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					context.getAssets().open("tablecolor.csv")));
			String readLine = null;
			readLine = br.readLine();
			try {
				while ((readLine = br.readLine()) != null) {
					Log.i("Data Input", readLine);
					String[] str = readLine.split(",");
					ContentValues values = new ContentValues();
					values.put(COLUMN_ID, Integer.parseInt(str[0]));
				    values.put(COLUMN_PIC, str[1]);
				    values.put(COLUMN_RED, Integer.parseInt(str[2]));
				    values.put(COLUMN_GREEN, Integer.parseInt(str[3]));
				    values.put(COLUMN_BLUE, Integer.parseInt(str[4]));
					
				    db.insert(TABLE_NAME, null, values);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// Getting All Color
	public Vector<getColor> getAllColor() {
		try {
			color_list.clear();
			// Select All Query
			String selectQuery = "SELECT * FROM " + TABLE_NAME;
			SQLiteDatabase db = this.getWritableDatabase();
			Cursor cursor = db.rawQuery(selectQuery, null);
			// looping through all rows and adding to list
			if (cursor.moveToFirst()) {
				do {
					getColor color = new getColor(Integer.parseInt(cursor
							.getString(0)), cursor.getString(1),
							Integer.parseInt(cursor.getString(2)),
							Integer.parseInt(cursor.getString(3)),
							Integer.parseInt(cursor.getString(4)));
					color_list.add(color);
				} while (cursor.moveToNext());
			}
			// return contact list
			cursor.close();
			db.close();
			return color_list;
		} catch (Exception e) {
			// TODO: handle exception
			Log.e("all_records", "" + e);
		}
		return color_list;
	}
	
	//	import the quiz data(from file .csv) into database.
	public void createTableQuiz(){
		SQLiteDatabase db = this.getWritableDatabase();
		try {  
            BufferedReader br = new BufferedReader(new InputStreamReader(context.getAssets().open("tablequiz2.csv")));  
            String readLine = null;  
            readLine = br.readLine();    
            try {  
                while ((readLine = br.readLine()) != null) {  
                	Log.i("Data Input", readLine);
                    String[] str = readLine.split(",");  
                    ContentValues values = new ContentValues();
					values.put(COLUMN2_ID, Integer.parseInt(str[0]));
				    values.put(COLUMN2_QUIZ, str[1]);
				    values.put(COLUMN2_ANS, str[2]);
					
				    db.insert(TABLE2_NAME, null, values);
                    
                }  
            } catch (IOException e) {  
                e.printStackTrace();  
            }  
        } catch (IOException e) {  
            e.printStackTrace();  
        } 
	}
	
	// Getting All Quiz
	public Vector<getQuiz> getAllQuiz() {
        try {
            quiz_list.clear();
            // Select All Query
            String selectQuery = "SELECT * FROM " + TABLE2_NAME;
            SQLiteDatabase db = this.getWritableDatabase();
            Cursor cursor = db.rawQuery(selectQuery, null);
            // looping through all rows and adding to list
            if (cursor.moveToFirst()) {
                do {
                    getQuiz quiz = new getQuiz(
                            Integer.parseInt(cursor.getString(0)),
                            cursor.getString(1),
                            cursor.getString(2)
                    );
                   quiz_list.add(quiz);
                } while (cursor.moveToNext());
            }
            // return contact list
            cursor.close();
            db.close();
            return quiz_list;
        } catch (Exception e) {
            // TODO: handle exception
            Log.e("all_records", "" + e);
        }
        return quiz_list;
    }
}