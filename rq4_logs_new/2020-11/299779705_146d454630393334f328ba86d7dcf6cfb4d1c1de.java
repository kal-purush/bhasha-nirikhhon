package com.example.cmput301f20t18;

import android.graphics.Bitmap;
import android.graphics.Matrix;
import android.os.Build;

import androidx.annotation.RequiresApi;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Base64;

public class photoAdapter {
    
    /**
     * Converts a byte representing a photo to a String
     * @param photo The byte[] object representation of a photo
     */
    @RequiresApi(api = Build.VERSION_CODES.O)
    public static String byteToString(byte[] photo){
        return Base64
                .getEncoder()
                .encodeToString(photo);
    }

    /**
     * Converts a String representing a photo to a byte[]
     * @param photo The String object representation of a photo
     */
    @RequiresApi(api = Build.VERSION_CODES.O)
    public byte[] stringToByte(String photo){
        return Base64.getDecoder().decode(photo);
    }

    @RequiresApi(api = Build.VERSION_CODES.O)
    public static String bitmapToString(Bitmap bitmap){
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        bitmap.compress(Bitmap.CompressFormat.PNG, 100, stream);
        byte[] byteArray = stream.toByteArray();
        return byteToString(byteArray);

    }

    public static Bitmap scaleBitmap(Bitmap bitmap, float newWidth, float newHeight){
        int width = bitmap.getWidth();
        int height = bitmap.getHeight();

        float scaleWidth = newWidth / width;
        float scaleHeight = newHeight / height;

        Matrix matrix = new Matrix();
        matrix.postScale(scaleWidth, scaleHeight);

        Bitmap finalMap = Bitmap
                .createBitmap(bitmap, 0, 0, width, height, matrix, false)
                .copy(Bitmap.Config.ARGB_8888, true);

        return finalMap;

    }
}