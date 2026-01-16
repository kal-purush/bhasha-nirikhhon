package com.example.planties.core.utils;

import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.Context;
import android.database.Cursor;
import android.graphics.Bitmap;
import android.graphics.Matrix;
import android.media.ExifInterface;
import android.net.Uri;
import android.provider.MediaStore;
import android.util.Log;
import android.widget.ImageView;

import java.io.File;
import java.io.IOException;

public class ImageUtils {

    private static final String TAG = "ImageUtils";

    public static void displayImage(Context context, Uri imageUri, ImageView imageView) {
        try {
            Bitmap bitmap = MediaStore.Images.Media.getBitmap(context.getContentResolver(), imageUri);
            int rotation = getRotationFromExif(context, imageUri);
            Bitmap rotatedBitmap = rotateBitmap(bitmap, rotation);
            imageView.setImageBitmap(rotatedBitmap);
        } catch (IOException e) {
            Log.e(TAG, "Error displaying image: " + e.getMessage(), e);
        }
    }

    private static int getRotationFromExif(Context context, Uri imageUri) {
        try {
            ExifInterface exif = new ExifInterface(context.getContentResolver().openInputStream(imageUri));
            int orientation = exif.getAttributeInt(ExifInterface.TAG_ORIENTATION, ExifInterface.ORIENTATION_NORMAL);
            switch (orientation) {
                case ExifInterface.ORIENTATION_ROTATE_90:
                    return 90;
                case ExifInterface.ORIENTATION_ROTATE_180:
                    return 180;
                case ExifInterface.ORIENTATION_ROTATE_270:
                    return 270;
                default:
                    return 0;
            }
        } catch (IOException e) {
            Log.e(TAG, "Error reading Exif data: " + e.getMessage(), e);
            return 0;
        }
    }

    private static Bitmap rotateBitmap(Bitmap source, int rotation) {
        Matrix matrix = new Matrix();
        matrix.postRotate(rotation);
        return Bitmap.createBitmap(source, 0, 0, source.getWidth(), source.getHeight(), matrix, true);
    }

    public static void deleteImage(Context context, long imageId) {
        // Query the MediaStore to get the file path of the image
        String[] projection = {MediaStore.Images.Media.DATA};
        Uri imageUri = ContentUris.withAppendedId(MediaStore.Images.Media.EXTERNAL_CONTENT_URI, imageId);

        ContentResolver contentResolver = context.getContentResolver();
        Cursor cursor = contentResolver.query(imageUri, projection, null, null, null);

        if (cursor != null && cursor.moveToFirst()) {
            int columnIndex = cursor.getColumnIndexOrThrow(MediaStore.Images.Media.DATA);
            String filePath = cursor.getString(columnIndex);

            // Delete the file using File class
            File file = new File(filePath);
            boolean deleted = file.delete();
            if (deleted) {
                Log.d("ImageUtils", "Image deleted successfully");
                // You may also want to notify the MediaStore that the image has been deleted
                contentResolver.delete(imageUri, null, null);
            } else {
                Log.e("ImageUtils", "Failed to delete image");
            }

            cursor.close();
        }
    }
}