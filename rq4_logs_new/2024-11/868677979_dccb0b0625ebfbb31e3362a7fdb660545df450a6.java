package com.tutorial.travel.Helpers;

import android.content.Context;
import android.graphics.BitmapFactory;
import android.os.Environment;
import android.graphics.Bitmap;
import android.widget.Toast;

import androidx.annotation.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class ImageHelpers {
    public static String saveImageToLocal(Bitmap bitmap, Context context) {
        FileOutputStream outputStream;
        try {
            // Define the storage directory and file name
            File storageDir = new File(context.getExternalFilesDir(Environment.DIRECTORY_PICTURES), "MyAppImages");
            if (!storageDir.exists()) {
                storageDir.mkdirs(); // Create directory if it doesn't exist
            }

            // Create a file nainme for the image
            String fileName = "IMG_" + System.currentTimeMillis() + ".jpg";
            File imageFile = new File(storageDir, fileName);

            // Save the bitmap to the file
            outputStream = new FileOutputStream(imageFile);
            bitmap.compress(Bitmap.CompressFormat.JPEG, 100, outputStream);
            outputStream.flush();
            outputStream.close();

            // Image saved successfully
            Toast.makeText(context, "Image saved to: " + imageFile.getAbsolutePath(), Toast.LENGTH_LONG).show();

            return fileName;
        } catch (IOException e) {
            e.printStackTrace();
            Toast.makeText(context, "Failed to save image", Toast.LENGTH_SHORT).show();
        }
        return "";
    }

    public static Bitmap getImageFromStorage(Context context, String fileName) {
        // Define the storage directory
        File storageDir = new File(context.getExternalFilesDir(Environment.DIRECTORY_PICTURES), "MyAppImages");

        // Create the file for the image
        File imageFile = new File(storageDir, fileName);

        // Check if the file exists
        if (imageFile.exists()) {
            // Decode the file into a Bitmap
            return BitmapFactory.decodeFile(imageFile.getAbsolutePath());
        } else {
            // Handle the case where the file does not exist
            return null;
        }
    }
}