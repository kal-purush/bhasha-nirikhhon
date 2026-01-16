package com.money.mmproject;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by Lukatz on 03.12.2015.
 */
public class TransactionsDB extends SQLiteOpenHelper {

        private static String DBNAME = "transactiondb";
        private static int VERSION = 1;
        public static final String FIELD_DATE = "transdate";
        public static final String FIELD_AMOUNT = "amount";
        public static final String FIELD_CATEGORY = "category";
        public static final String FIELD_DESCRIPTION = "description";
        private static final String DATABASE_TABLE = "transactions";


        private static final String DATABASE_TABLE_USERDATA = "userdata";




        private SQLiteDatabase mDB;

        public TransactionsDB(Context context) {
            super(context, DBNAME, null, VERSION);
            this.mDB = getWritableDatabase();
        }


        @Override
        public void onCreate(SQLiteDatabase db) {
            String sql =     "create table " + DATABASE_TABLE + " ( " +
                    FIELD_DATE + " integer primary key autoincrement , " +
                    FIELD_AMOUNT + " double , " +
                    FIELD_CATEGORY + " text , " +
                    FIELD_DESCRIPTION + " text " +
                    " ) ";
            db.execSQL(sql);
        }

        public long insert(ContentValues contentValues){
            long rowID = mDB.insert(DATABASE_TABLE, null, contentValues);
            System.out.println("HIER BIN ICH");
            Cursor c=mDB.rawQuery("SELECT * FROM "+DATABASE_TABLE,null);
            DatabaseUtils dbu = new DatabaseUtils();
            DatabaseUtils.dumpCursor(c);
            return rowID;
        }

        public int del(){
            int cnt = mDB.delete(DATABASE_TABLE, null, null);
            return cnt;
        }

        //public Cursor getAllLocations(){
        //    return mDB.query(DATABASE_TABLE, new String[] { FIELD_ROW_ID,  FIELD_LAT , FIELD_LNG, FIELD_ZOOM } , null, null, null, null, null);
        //}

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        }
}