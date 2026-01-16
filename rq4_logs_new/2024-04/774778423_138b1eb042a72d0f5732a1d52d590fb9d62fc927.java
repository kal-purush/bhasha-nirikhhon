package com.example.duan1chinhthuc.DAO;

import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

import com.example.duan1chinhthuc.Database.DbHelper;

public class DAO_thongke {

    private final DbHelper dbHelper;

    public DAO_thongke(Context context) {
        dbHelper = new DbHelper(context);
    }
    public int getDoanhThu(String ngaybatdau, String ngayketthuc){//2022/09/30
        ngaybatdau = ngaybatdau.replace("/","");
        ngayketthuc = ngayketthuc.replace("/","");
        SQLiteDatabase db = dbHelper.getReadableDatabase();
        Cursor cursor = db.rawQuery("select sum(tienthue) from phieumuon where substr(ngay,7) ||substr(ngay, 4,2) || substr(ngay,1,2) between ? and ?", new String[]{ngaybatdau, ngayketthuc});
        if (cursor.getCount() > 0){
            cursor.moveToFirst();
            return cursor.getInt(0);
        }
        return 0;
    }
}