package com.bankfinder.chathurangasandun.bankfinder;

import android.content.ContentValues;
import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import com.bankfinder.chathurangasandun.bankfinder.model.Branches;

/**
 * Created by Chathuranga Sandun on 4/14/2016.
 */
public class DatabaseOpenHelper extends SQLiteOpenHelper {

    private static final int DATABASE_VERSION = 1;

    // Database Name
    private static final String DATABASE_NAME = "bank.db";

    // marks table name
    public static final String TABLE_BRANCH = "branches";

    // marks Table Columns names
    public static final String KEY_ID = "_id";
    public static final String KEY_BANK = "bank";
    public static final String KEY_NAME="name";
    public static final String KEY_LAT= "lat";
    public static final String KEY_LONG= "long";
    public static final String KEY_ADRESS ="address";
    public static final String KEY_TP ="tp";
    public static final String KEY_WEEKOPEN ="weekopen";
    public static final String KEY_WEEKCLOSE="weekclose";
    public static final String KEY_SATOPEN ="satopen";
    public static final String KEY_SATCLOSE ="satclose";


    public static final String CREATE_BRANCHES_TABLE =
            "CREATE TABLE "+TABLE_BRANCH+" ("
                    + KEY_ID+" INTEGER PRIMARY KEY,"
                    +KEY_BANK+" TEXT,"
                    +KEY_NAME+" TEXT,"
                    +KEY_LAT+" REAL,"
                    +KEY_LONG+" REAL,"
                    +KEY_ADRESS+" TEXT,"
                    +KEY_TP+" TEXT,"
                    +KEY_WEEKOPEN +" TEXT,"
                    +KEY_WEEKCLOSE+" TEXT,"
                    +KEY_SATOPEN +" TEXT,"
                    +KEY_SATCLOSE +" TEXT)";


    public DatabaseOpenHelper(Context context) {
        super(context,DATABASE_NAME,null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS "+ TABLE_BRANCH);
        db.execSQL(CREATE_BRANCHES_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS " + TABLE_BRANCH);
        onCreate(db);
    }

    public void addBranch(Branches branches) {
        SQLiteDatabase db = this.getReadableDatabase();

        ContentValues values = new ContentValues();
        values.put(this.KEY_ID,branches.getId());
        values.put(this.KEY_BANK,branches.getBank());
        values.put(this.KEY_NAME,branches.getName() );
        values.put(this.KEY_LAT,branches.getLatitude() );
        values.put(this.KEY_LONG,branches.getLongtitude() );
        values.put(this.KEY_ADRESS,branches.getAddress() );
        values.put(this.KEY_TP,branches.getTp() );
        values.put(this.KEY_WEEKOPEN,branches.getWeekOpen() );
        values.put(this.KEY_WEEKCLOSE,branches.getWeekClose() );
        values.put(this.KEY_SATOPEN,branches.getSatOpen() );
        values.put(this.KEY_SATCLOSE,branches.getSatClose() );


        // Inserting Row
        db.insert(this.TABLE_BRANCH, null, values);
        db.close(); // Closing database connection
    }
}