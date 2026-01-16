package sw19.moring03.paint.utils;

import android.graphics.Bitmap;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class BitmapCompressor {
    private String imageName = "image.png";

    public File compressBitmapToPNG(File path, Bitmap bitmap) {
        try {
            path.mkdirs();
            FileOutputStream stream = new FileOutputStream(path + "/" + imageName);
            bitmap.compress(Bitmap.CompressFormat.PNG, 100, stream);
            stream.close();
            return new File(path, imageName);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}