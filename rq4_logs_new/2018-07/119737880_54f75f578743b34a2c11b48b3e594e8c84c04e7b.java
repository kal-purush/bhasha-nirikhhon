package com.softwareoverflow.colorfall.game_pieces;

import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;

import java.util.HashMap;
import java.util.Map;

public class Bitmaps {

    private static Map<Colour, Bitmap> bitmapMap = new HashMap<>();

    public static void initialize(Context context, int bitmapSize){
        for(Colour colour : Colour.values()){
            Bitmap bitmap = BitmapFactory.decodeResource(context.getResources(), colour.getBitmapRef());
            bitmap = Bitmap.createScaledBitmap(bitmap, bitmapSize, bitmapSize, true);
            bitmapMap.put(colour, bitmap);
        }
    }

    public static Bitmap getBitmap(Colour colour){
        return bitmapMap.get(colour);
    }
}