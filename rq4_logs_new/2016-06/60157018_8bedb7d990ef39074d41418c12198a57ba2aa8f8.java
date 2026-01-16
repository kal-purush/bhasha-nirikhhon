package com.example.gposabella.basic;

import android.graphics.Bitmap;

/**
 * Created by giovanni on 6/2/16.
 */
public class Heap {
    private static Bitmap bitmap = null;

    public static Bitmap getBitmap() {
        return bitmap;
    }

    public static void setBitmap(Bitmap m) {
        bitmap = m;
    }

}