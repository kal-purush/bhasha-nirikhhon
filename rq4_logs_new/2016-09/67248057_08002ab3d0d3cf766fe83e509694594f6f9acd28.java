package com.example.ahmed.popularmoviesapp;


import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import java.util.ArrayList;

public class DataBaseHandler extends SQLiteOpenHelper {

    public static final int VERSION = 1;
    public static final String DBNAME = "favorites.db";

    public DataBaseHandler(Context context) {
        super(context, DBNAME, null, VERSION);

    }
    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("create table IF NOT EXISTS favorites" +
                " (id TEXT primary key" +
                ",poster_path TEXT,date TEXT,rate TEXT" +
                ",overview TEXT,title TEXT)");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("Drop table if EXISTS favorites");
        onCreate(db);

    }


    public void addMovie(String id ,String poster_path,String date,String rate , String overview,String title){
        SQLiteDatabase db = this.getWritableDatabase();
        ContentValues contentValues = new ContentValues();
        contentValues.put("id",id);
        contentValues.put("poster_path",poster_path);
        contentValues.put("date",date);
        contentValues.put("rate",rate);
        contentValues.put("overview", overview);
        contentValues.put("title", title);

        db.insert("favorites", null, contentValues);
    }

    public ArrayList<Movie> getAllMovies(){

        //int i=0;
       // String res_id,res_url,res_date,res_rate,res_description,res_title;
        //Movie movieInfo[] = new Movie[getMoviesCount()];
        ArrayList<Movie>  movieArrayList=new ArrayList<>();
        SQLiteDatabase sqLiteDatabase = this.getReadableDatabase();
        Cursor cursor = sqLiteDatabase.rawQuery("select * from favorites",null);
        cursor.moveToFirst();
        //Log.d("Size l array kollo " , String.valueOf(movieInfo.length));

        while (!cursor.isAfterLast()) {
            movieArrayList.add(new Movie(cursor.getString(cursor.getColumnIndex("title")),
                    cursor.getString(cursor.getColumnIndex("overview")),
                    cursor.getString(cursor.getColumnIndex("poster_path")),
                    cursor.getString(cursor.getColumnIndex("date")),
                    cursor.getString(cursor.getColumnIndex("rate")),
                    cursor.getString(cursor.getColumnIndex("id"))
            ));
            cursor.moveToNext();
        }
            /*
            res_id =res.getString(res.getColumnIndex("id"));
            res_url = res.getString(res.getColumnIndex("url"));
            res_date = res.getString(res.getColumnIndex("date"));
            res_rate  = res.getString(res.getColumnIndex("rate"));
            res_description = res.getString(res.getColumnIndex("description"));
            res_title = res.getString(res.getColumnIndex("title"));
          //  if(i==movieInfo.length)
             //   break;
           // movieInfo[i] = new Movie(res_title,res_description,res_url,res_date,res_rate,res_id);
            res.moveToNext();
         //   i++;
            //Log.d("Salama",""+i);
        }
        return new ArrayList<Movie>(Arrays.asList(movieInfo));
        */
            return movieArrayList;
    }

    public int getMoviesCount() {
        String countQuery = "SELECT * FROM favorites" ;
        SQLiteDatabase db = this.getReadableDatabase();
        Cursor cursor = db.rawQuery(countQuery, null);
        int cnt = cursor.getCount();
        cursor.close();
        return cnt;
    }



    public boolean removeMovie(String id){
        String query = "DELETE FROM favorites WHERE id="+id;
        SQLiteDatabase db = this.getWritableDatabase();
        db.execSQL(query);
        return false;
    }

    public boolean isFavorite(String id){
        String query = "SELECT * FROM favorites WHERE id="+id;
        SQLiteDatabase db = this.getReadableDatabase();
        Cursor cursor = db.rawQuery(query,null);
        if(cursor.getCount()!=0)
            return true;
        else
            return false;
    }

}