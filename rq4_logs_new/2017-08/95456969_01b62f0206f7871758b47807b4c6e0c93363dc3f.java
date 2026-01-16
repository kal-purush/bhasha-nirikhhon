package com.example.edelsteindo.androidproject.Model;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Dor-New on 11/08/2017.
 */

public class UserPostSql {
    private static final String USER_POST_TABLE = "user_post";

    private static final String USER_POST_ID = "id";
    private static final String USER_POST_POST_ID = "post_id";
    private static final String USER_POST_USER_EMAIL = "user_email";


    static void updateAllUsers(SQLiteDatabase db, Post post) {
        List<String> users = getAllUsers(db,post.getId());
        List<String> postUsers = new LinkedList<String>();
        ContentValues values;

        postUsers.addAll(post.getLikedUsers());
        postUsers.removeAll(users);

        for (String email: postUsers) {
            values = new ContentValues();

            values.put(USER_POST_POST_ID, post.getId());
            values.put(USER_POST_USER_EMAIL, email);

            db.insert(USER_POST_TABLE, null, values);
        }

        postUsers.addAll(post.getLikedUsers());
        users.removeAll(postUsers);

        for (String email: users) {
            removeLike(db,post.getId(),email);
        }
    }

    static void removePost(SQLiteDatabase db, String postId) {
        db.execSQL("delete from "+USER_POST_TABLE+" where "+USER_POST_POST_ID+"="+"'"+postId+"';");
    }

    static void removeLike(SQLiteDatabase db, String postId, String email) {
        db.execSQL("delete from "+USER_POST_TABLE+" where "+USER_POST_POST_ID+"="+"'"+postId+"' and "+USER_POST_USER_EMAIL+"="+"'"+email+"'"+";");
    }

    static List<String> getAllUsers(SQLiteDatabase db, String postId) {
        String[] col = {USER_POST_ID,USER_POST_POST_ID,USER_POST_USER_EMAIL};
        Cursor cursor = db.query(USER_POST_TABLE , col, USER_POST_POST_ID+"=?", new String[] {postId}, null, null, null);

        List<String> list = new LinkedList<String>();

        if (cursor.moveToFirst()) {
            int descriptionIndex = cursor.getColumnIndex(USER_POST_USER_EMAIL);

            do {
                list.add(cursor.getString(descriptionIndex));
            } while (cursor.moveToNext());
        }
        return list;
    }

    static public void onCreate(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE " + USER_POST_TABLE +
                " (" +
                USER_POST_ID + " NUMBER PRIMARY KEY, " +
                USER_POST_POST_ID + " TEXT, " +
                USER_POST_USER_EMAIL + " TEXT "+
                ");");
    }

    static public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS " + USER_POST_TABLE + ";");
        onCreate(db);
    }
}