package com.ijzepeda.contentprovider;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.support.annotation.Nullable;
import android.text.TextUtils;

import java.util.HashMap;

/**
 * Created by ivan.zepeda on 08/11/2016.
 */

public class GameProvider extends ContentProvider {
static final String PROVIDER_NAME ="com.ijzepeda.ContentProvider.GameProvider";
static final String URL="content://"+PROVIDER_NAME+"/games";
static final Uri CONTENT_URI=Uri.parse(URL);
static final String _ID="_id";
static final String NAME="name";
static final String GAME_ID="game_id";
static final int GAMES=1;
static final int GAMES_ID=2;
private static HashMap<String,String> GAMES_PROJECTION_MAP;
    static final UriMatcher uriMatcher;
    static{
        uriMatcher=new UriMatcher(UriMatcher.NO_MATCH);
        uriMatcher.addURI(PROVIDER_NAME,"games",GAMES);
        uriMatcher.addURI(PROVIDER_NAME,"games/#",GAMES_ID);
    }
    /*Database stuff*/
private SQLiteDatabase database;
static final String DATABASE_NAME="broastedGames";
static final String GAMES_TABLE_NAME="games";
static final int DATABASE_VERSION=1;
static final String CREATE_DB_TABLE= "CREATE TABLE "+ GAMES_TABLE_NAME +
        " (_id INTEGER PRIMARY KEY AUTOINCREMENT, "+
        " name TEXT NOT NULL,"+
        " grade TEXT NOT NULL);";

//Helper class creates and manage the provider data repository

private static class DatabaseHelper extends SQLiteOpenHelper{

    public DatabaseHelper(Context context){
        super(context, DATABASE_NAME,null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
db.execSQL(CREATE_DB_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
db.execSQL("DROP TABLE IF EXISTS "+GAMES_TABLE_NAME);
        onCreate(db);
    }
}



    //Overrided methods

    @Override
    public boolean onCreate() {
      Context context=getContext();
        DatabaseHelper dbHelper=new DatabaseHelper(context);
        database=dbHelper.getWritableDatabase();
        return(database==null)?false:true;
    }

    @Nullable
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        SQLiteQueryBuilder queryBuilder= new SQLiteQueryBuilder();
        queryBuilder.setTables(GAMES_TABLE_NAME);

        switch(uriMatcher.match(uri)){   //something with the urimatcher from above
            case GAMES:
                queryBuilder.setProjectionMap(GAMES_PROJECTION_MAP);
                break;
            case GAMES_ID:
                queryBuilder.appendWhere(_ID+"="+ uri.getPathSegments().get(1));
                break;
            default:
        }
if(sortOrder==null||sortOrder.equals("")){
    sortOrder=NAME;
}
        Cursor cursor=queryBuilder.query(database,projection,selection,selectionArgs,null,null,sortOrder);
        cursor.setNotificationUri(getContext().getContentResolver(),uri);
        return cursor;
    }

    @Nullable
    @Override
    public String getType(Uri uri) {
        switch(uriMatcher.match(uri)){
            case GAMES:
                return "vnd.android.cursor.dir/vnd.example.students";
//                return "com.android.cursor.dir/com.ijzepeda.games";
            case GAMES_ID:
                return "vnd.android.cursor.item/vnd.example.students";
//                return "com.android.cursor.item/com.ijzepeda.games";
            default:
                throw new IllegalArgumentException("Uknown URI "+uri);
        }

    }

    @Nullable
    @Override
    public Uri insert(Uri uri, ContentValues values) {
      long rowId=database.insert(GAMES_TABLE_NAME,"",values);
        if(rowId>0){
            Uri _uri= ContentUris.withAppendedId(CONTENT_URI,rowId);
            getContext().getContentResolver().notifyChange(_uri,null);
            return _uri;
        }
        throw new SQLException("Failed to add record into "+uri);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
       int count = 0;
        switch(uriMatcher.match(uri)){
            case GAMES_ID:
                String id=uri.getPathSegments().get(1);
                count= database.delete(GAMES_TABLE_NAME,
                        _ID+" = "+id+
                        (!TextUtils.isEmpty(selection)?
                        " AND ("+ selection+')': ""),
                        selectionArgs);
                break;
            case GAMES:
                count = database.delete(GAMES_TABLE_NAME,selection,selectionArgs);
                break;
                default:
                    throw new IllegalArgumentException("Uknown URI "+uri);
        }
        getContext().getContentResolver().notifyChange(uri,null);
        return count;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
int count=0;
    switch(uriMatcher.match(uri)){
        case GAMES:
            count=database.update(GAMES_TABLE_NAME,values,selection,selectionArgs);
            break;
        case GAMES_ID:
            count= database.update(GAMES_TABLE_NAME,values,
                    _ID+" = "+
            uri.getPathSegments().get(1)+
                    (!TextUtils.isEmpty(selection)?
                    " AND ("+selection+")":"")
                    ,selectionArgs);

            break;
        default:
            throw new IllegalArgumentException("Uknown URI "+uri);
        }
    getContext().getContentResolver().notifyChange(uri,null);
        return count;
    }

}