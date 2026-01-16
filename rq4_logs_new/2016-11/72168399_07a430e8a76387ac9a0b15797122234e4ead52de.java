package com.luxshare.utils.utils;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Matrix;
import android.graphics.PixelFormat;
import android.graphics.drawable.Drawable;
import android.util.Log;
import android.view.View;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by Luxshare-ICT on 2016/11/29.
 */

public class BitmapUitls {

    private static final String TAG = "BitmapUitls";

    /**
     * 将View转换为bitmap
     *
     * @param view
     * @return
     */
    public static Bitmap getBitmapFromView(View view) {

        // Define a bitmap with the same size as the view
        Bitmap bitmap = Bitmap.createBitmap(view.getWidth(), view.getHeight(), Bitmap.Config.ARGB_8888);
        // Bind a canvas to it
        Canvas canvas = new Canvas(bitmap);
        // get the view's background
        Drawable background = view.getBackground();
        if (background != null) {
            // has background drawable,then draw it on the canvas
            background.draw(canvas);
        } else {
            // does not have background drawable,then draw white background on the canvas
            canvas.drawColor(Color.WHITE);
        }
        // draw the view on the canvas
        view.draw(canvas);
        return bitmap;
    }

    /**
     * 将view转换为bitmap
     *
     * @param view
     * @return
     */
    public static Bitmap viewToBitmap(View view) {
        view.setDrawingCacheEnabled(true);
        view.buildDrawingCache();
        Bitmap bm = view.getDrawingCache();
        return bm;
    }


    /**
     * drawable 转 bitmap
     *
     * @param drawable
     * @return
     */
    public static Bitmap drawable2Bitmap(Drawable drawable) {
        Bitmap bitmap = Bitmap.createBitmap(drawable.getIntrinsicWidth(), drawable.getIntrinsicHeight(), drawable.getOpacity() != PixelFormat.OPAQUE ? Bitmap.Config.ARGB_8888 : Bitmap.Config.RGB_565);
        Canvas canvas = new Canvas(bitmap);
        drawable.setBounds(0, 0, drawable.getIntrinsicWidth(), drawable.getIntrinsicHeight());
        drawable.draw(canvas);
        return bitmap;
    }


    /**
     * 缩放图片
     *
     * @param bitmapPath 图片路径
     * @param newWidth   新的宽度
     * @param newHeight  新的高度
     * @return
     */
    public static Bitmap zoomBitmap(String bitmapPath, float newWidth, float newHeight) {
        Bitmap bitmap = BitmapFactory.decodeFile(bitmapPath);
        if (bitmap == null) {
            Log.i(TAG, "zoomBitmap: 获取bitmap失败");
            return null;
        }
        int width = bitmap.getWidth();
        int height = bitmap.getHeight();
        float scaleWidth = newWidth / width;
        float scaleHeight = newHeight / height;
        Matrix matrix = new Matrix();
        matrix.postScale(scaleWidth, scaleHeight);
        return Bitmap.createBitmap(bitmap, 0, 0, width, height, matrix, true);
    }

    /**
     * 缩放图片
     *
     * @param bitmap    图片资源
     * @param newWidth  新的高度
     * @param newHeight 新的宽度
     * @return
     */
    public static Bitmap zoomBitmap(Bitmap bitmap, float newWidth, float newHeight) {
        if (bitmap == null) {
            Log.i(TAG, "zoomBitmap: 获取bitmap失败");
            return null;
        }
        int width = bitmap.getWidth();
        int height = bitmap.getHeight();
        float scaleWidth = newWidth / width;
        float scaleHeight = newHeight / height;
        Matrix matrix = new Matrix();
        matrix.postScale(scaleWidth, scaleHeight);
        return Bitmap.createBitmap(bitmap, 0, 0, width, height, matrix, true);
    }


    /**
     * 将图片保存到本地的时候进行压缩,即将图片从bitmap转换为file形式时进行压缩
     * 特点:File形式的图片确实被压缩了,但是重新读取压缩后的file为bitmap的时候,它所占用的内存并没有改变
     *
     * @param bitmap
     * @param file
     */
    public static void compressBitmap2File(Bitmap bitmap, File file) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int options = 80;
        bitmap.compress(Bitmap.CompressFormat.JPEG, options, baos);
        while (baos.toByteArray().length / 1024 > 100) {
            baos.reset();
            options -= 1;
            bitmap.compress(Bitmap.CompressFormat.JPEG, options, baos);
        }

        try {
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(baos.toByteArray());
            fos.flush();
            fos.close();
            baos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static Bitmap compressImageFromFile(String filePath, float pixWidth, float pixHeight) {
        BitmapFactory.Options options = new BitmapFactory.Options();
        options.inJustDecodeBounds = true; // 只读边,不读内容
        Bitmap bitmap = BitmapFactory.decodeFile(filePath, options);

        options.inJustDecodeBounds = false;
        int w = options.outWidth;
        int h = options.outHeight;
        int scale = 1;
        if (w > h && w > pixWidth) {
            scale = (int) (options.outWidth / pixWidth);
        } else if (w < h && h > pixHeight) {
            scale = (int) (options.outHeight / pixHeight);
        }
        if (scale < 0) {
            scale = 1;
        }
        options.inSampleSize = scale; // 设置采样率

        options.inPreferredConfig = Bitmap.Config.ARGB_8888;// 该模式是默认的,可以不设置
        options.inPurgeable = true; // 同时设置才会有效
        options.inInputShareable = true; // 当系统内存不够时候图片会自动被回收

        bitmap = BitmapFactory.decodeFile(filePath, options);
        return bitmap;
    }
}