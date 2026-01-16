package com.example.administrator.smallvault.db;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;

import com.example.administrator.smallvault.db.entity.Shouru;
import com.example.administrator.smallvault.db.entity.Zhichu;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2016/5/5.
 */
public class DBHelper {

    private static volatile DBHelper intance = null;
    private ContentResolver mContentResolver;
    private static Context context;

    private DBHelper(Context context) {
        this.mContentResolver = context.getContentResolver();
    }

    public static DBHelper getIntance(Context context) {

        if (intance == null) {
            synchronized (DBHelper.class) {
                if (intance == null) {
                    intance = new DBHelper(context);
                }
            }
        }
        return intance;
    }

    public boolean inster(List<Zhichu> list) {
        try {
            for (Zhichu entity : list) {
                ContentValues values = new ContentValues();
                values.put("time", entity.getTime());
                values.put("food", entity.getFood());
                values.put("shopping", entity.getShopping());
                values.put("play", entity.getPlay());
                values.put("medicine", entity.getMedicine());
                values.put("other", entity.getOther());
                mContentResolver.insert(PayContentProvider.CONTENT_URI_ZHICHU, values);
                values = null;
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean inster(Zhichu entity) {
        try {
            ContentValues values = new ContentValues();
            values.put("time", entity.getTime());
            values.put("food", entity.getFood());
            values.put("shopping", entity.getShopping());
            values.put("play", entity.getPlay());
            values.put("medicine", entity.getMedicine());
            values.put("other", entity.getOther());
            mContentResolver.insert(PayContentProvider.CONTENT_URI_ZHICHU, values);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean inster(Shouru entity) {
        try {
            ContentValues values = new ContentValues();
            values.put("time", "2016-05-05");
            values.put("shouru", 15);
            mContentResolver.insert(PayContentProvider.CONTENT_URI_SHOURU, values);
            return true;
        } catch (Exception e) {
            return false;
        }
    }


    public List<Shouru> queryShouru() {
        List<Shouru> list = new ArrayList<Shouru>();
        Cursor cursor = mContentResolver.query(PayContentProvider.CONTENT_URI_SHOURU, null, null, null, null);
        cursor.moveToFirst();
        while (!cursor.isAfterLast()) {
            Shouru entity = new Shouru();
//            String table = cursor.getString(cursor.getColumnIndex("table_name"));
            entity.setTime(cursor.getString(cursor.getColumnIndex("time")));
            entity.setShouru(cursor.getString(cursor.getColumnIndex("shouru")));
            list.add(entity);
            cursor.moveToNext();
        }
        cursor.close();
        return list;
    }


    public ArrayList<Zhichu> queryZhichu() {
        ArrayList<Zhichu> list = new ArrayList<>();
        Cursor cursor = mContentResolver.query(PayContentProvider.CONTENT_URI_ZHICHU, null, null, null, null);
        cursor.moveToFirst();
        while (!cursor.isAfterLast()) {
            Zhichu entity = new Zhichu();

//            String table = cursor.getString(cursor.getColumnIndex("table_name"));
            entity.setTime(cursor.getString(cursor.getColumnIndex("time")));
            entity.setFood(cursor.getString(cursor.getColumnIndex("food")));
            entity.setShopping(cursor.getString(cursor.getColumnIndex("shopping")));
            entity.setPlay(cursor.getString(cursor.getColumnIndex("play")));
            entity.setMedicine(cursor.getString(cursor.getColumnIndex("medicine")));
            entity.setOther(cursor.getString(cursor.getColumnIndex("other")));
            list.add(entity);
            cursor.moveToNext();
        }
        cursor.close();
        return list;
    }


//    private void update() {
//        ContentValues values = new ContentValues();
//        values.put("detail", "my name is harvic !!!!!!!!!!!!!!!!!!!!!!!!!!");
//        int count = this.getContentResolver().update(mCurrentURI, values, "_id = 1", null);
//        Log.e("test ", "count=" + count);
//    }


//    private void delete() {
//        int count = this.getContentResolver().delete(mCurrentURI, "_id = 1", null);
//        Log.e("test ", "count=" + count);
//    }
}