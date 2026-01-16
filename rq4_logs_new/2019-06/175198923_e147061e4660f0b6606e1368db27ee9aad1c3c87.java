package sw19.moring03.paint.utils;

import android.graphics.Bitmap;
import android.view.View;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class ImageShare {
    private View view;

    public ImageShare(View view) {
        this.view = view;
    }

    public Boolean shareImage(Bitmap bitmap) {
        if (bitmap == null) {
            return false;
        }
        try {
            File cachePath = new File(view.getContext().getCacheDir(), "images");
            cachePath.mkdirs(); // don't forget to make the directory
            FileOutputStream stream = new FileOutputStream(cachePath + "/image.png"); // overwrites this image every time
            bitmap.compress(Bitmap.CompressFormat.PNG, 100, stream);
            stream.close();
            return true;

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}