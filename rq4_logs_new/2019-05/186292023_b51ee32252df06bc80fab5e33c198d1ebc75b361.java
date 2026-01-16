package pl.krakow.uek.util;

import android.content.Context;
import android.content.res.AssetManager;
import android.os.Environment;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PhotoPlusOcrUtil {

    public static File getOutputMediaFile(Context context) {
        // File mediaStorageDir = new File(context.getFilesDir().getAbsolutePath());
        File mediaStorageDir = context.getExternalFilesDir(Environment.DIRECTORY_PICTURES);

        mediaStorageDir = new File(mediaStorageDir, "pictures");
        if (!mediaStorageDir.exists()) {
            if (!mediaStorageDir.mkdirs()) {
                return null;
            }
        }

        String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String path = mediaStorageDir.getPath() + File.separator +
                "IMG_" + timeStamp + ".jpg";
        return new File(path);
    }


    public static void checkFile(AssetManager assetManager, File dir, String datapath) {
        if (!dir.exists()&& dir.mkdirs()){
            copyFiles(assetManager, datapath);
        }
        if(dir.exists()) {
            String datafilepath = datapath+ "/tessdata/eng.traineddata";
            File datafile = new File(datafilepath);
            if (!datafile.exists()) {
                copyFiles(assetManager, datapath);
            }
        }
    }

    private static void copyFiles(AssetManager assetManager, String datapath) {
        try {
            String filepath = datapath + "/tessdata/eng.traineddata";
            InputStream instream = assetManager.open("tessdata/eng.traineddata");
            OutputStream outstream = new FileOutputStream(filepath);
            byte[] buffer = new byte[1024];
            int read;
            while ((read = instream.read(buffer)) != -1) {
                outstream.write(buffer, 0, read);
            }
            outstream.flush();
            outstream.close();
            instream.close();
            File file = new File(filepath);
            if (!file.exists()) {
                throw new FileNotFoundException();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}