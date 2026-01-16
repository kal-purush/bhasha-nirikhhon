package com.yamblz.hardoperations.utils;

import android.graphics.Bitmap;
import android.graphics.Canvas;
import android.graphics.Matrix;
import android.graphics.Paint;
import android.support.annotation.NonNull;

//!!! Do not modify this !!!

/**
 * Created by i-sergeev on 06.07.16
 */
public final class BitmapUtils
{
    public static Bitmap fitToCenterBitmap(@NonNull Bitmap image, int width, int height)
    {
        Bitmap background = Bitmap.createBitmap(width, height, image.getConfig());
        float originalWidth = image.getWidth();
        float originalHeight = image.getHeight();
        Canvas canvas = new Canvas(background);
        float scale = width / originalWidth;
        float xTranslation = 0.0f, yTranslation = (height - originalHeight * scale) / 2.0f;
        Matrix transformation = new Matrix();
        transformation.postTranslate(xTranslation, yTranslation);
        transformation.preScale(scale, scale);
        Paint paint = new Paint();
        paint.setFilterBitmap(true);
        canvas.drawBitmap(image, transformation, paint);
        return background;
    }
}