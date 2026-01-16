package com.example.duan1chinhthuc;

import android.content.ContentValues;
import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.widget.Toast;

import com.example.duan1chinhthuc.Database.DbHelper; // Import DbHelper from the correct package

public class UpdatePassword {

    private Context context;
    private SQLiteDatabase db;

    // Constructor không tham số
    public UpdatePassword() {}

    public UpdatePassword(Context context) {
        this.context = context;
        DbHelper dbHelper = new DbHelper(context);
        this.db = dbHelper.getWritableDatabase();
    }

    public boolean updatePassword(String oldPassword, String newPassword) {
        ContentValues values = new ContentValues();
        values.put("matkhau", newPassword);

        int rowsAffected = db.update("thuthu", values, "matkhau = ?", new String[]{oldPassword});

        if (rowsAffected > 0) {
            return true;
        } else {
            return false;
        }
    }
}