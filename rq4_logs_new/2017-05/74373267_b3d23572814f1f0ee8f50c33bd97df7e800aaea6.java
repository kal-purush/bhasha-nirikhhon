package com.xjh1994.helloandroid.core.util;

import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.graphics.Bitmap;
import android.net.Uri;
import android.os.Environment;
import android.provider.MediaStore;

import com.orhanobut.logger.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import me.nereo.multi_image_selector.MultiImageSelector;

/**
 * Created by xjh1994 on 17/2/28.
 */

public class ImageUtils {

    public static final int REQUEST_IMAGE = 2;
    public static final int LIMIT = 9; //默认限制选图张数

    /**
     * 选图
     * @param activity
     * @param paths
     */
    public static void openGallery(Activity activity, ArrayList<String> paths) {
        openGallery(activity, paths, LIMIT);
    }

    /**
     * 选图
     * @param activity
     * @param paths
     * @param limit
     */
    public static void openGallery(Activity activity, ArrayList<String> paths, int limit) {
        MultiImageSelector.create()
                .showCamera(true) // show camera or not. true by default
                .count(limit) // max select image size, 9 by default. used width #.multi()
                .multi() // multi mode, default mode;
                .origin(paths) // original select data set, used width #.multi()
                .start(activity, REQUEST_IMAGE);
    }

    /**
     * 保存图片
     * @param context
     * @param bm
     * @param name
     */
    public static String saveImage(Context context, Bitmap bm, String name) {
        File appDir = new File(Environment.getExternalStorageDirectory(), "/download/");
        if (!appDir.exists()) {
            appDir.mkdir();
        }
        File file = new File(appDir, name);
        Logger.d(file.getName());
        try {
            FileOutputStream out = new FileOutputStream(file);
            bm.compress(Bitmap.CompressFormat.JPEG, 100, out);
            out.flush();
            out.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Uri uri = Uri.fromFile(file);
        // 通知图库更新
        Intent scannerIntent = new Intent(Intent.ACTION_MEDIA_SCANNER_SCAN_FILE, uri);
        context.sendBroadcast(scannerIntent);

        return file.getAbsolutePath();
    }

    /**
     * 从Uri获取图片真实地址
     * @param context
     * @param uri
     * @return
     */
    public static String getPathFromUri(Context context, Uri uri) {
        String path = uri.getPath();
        File file = new File(path);
        if (!file.exists()) {
            path = getImageLoad(context, uri);
        }

        return path;
    }

    public static String getImageLoad(Context context, Uri uri) {
        String[] proj = {MediaStore.Images.Media.DATA};
        String imagePath = null;
        Cursor cursor = context.getContentResolver().query(uri, proj, null, null, null);
        if (cursor != null) {
            int column_index = cursor
                    .getColumnIndexOrThrow(MediaStore.Images.Media.DATA);
            if (cursor.getCount() > 0 && cursor.moveToFirst()) {
                imagePath = cursor.getString(column_index);
            }
        }
        return imagePath;
    }
}