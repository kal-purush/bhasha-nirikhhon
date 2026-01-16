package com.example.danil.skilder;

import android.content.Context;
import android.graphics.Bitmap;
import android.os.Environment;
import android.provider.MediaStore;
import android.support.v4.util.LruCache;
import android.util.Log;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Created by danil on 02.11.16.
 */
public class FileHelper {
    private static final FileHelper ourInstance = new FileHelper();

    public static FileHelper getInstance() {
        return ourInstance;
    }

    public interface Callback{
        void onResult(boolean isSuccess);
    }

    private final Executor executor = Executors.newCachedThreadPool();

    private LruCache<String, String> cache = new LruCache<>(32);

    private Callback callback;

    public void setCallback(Callback callback) {
        this.callback = callback;
    }
    public void resetCallback(){
        this.callback = null;
    }
    public void saveCurrentScreen(final Context context) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                File directory = new File(Environment.getExternalStorageDirectory().getAbsolutePath().toString()); // Create imageDir
                File mypath = new File(directory, "test.png");
                FileOutputStream out = null;
                try {
                    out = new FileOutputStream(mypath);
                    Bitmap bmp = DrawStateManager.getInstance().getCurrentScreen();
                    bmp.compress(Bitmap.CompressFormat.PNG, 100, out);
                    MediaStore.Images.Media.insertImage(context.getContentResolver(), bmp, UUID.randomUUID().toString()+".png", "drawing");
                    notifyResult(true);
                } catch (Exception e) {
                    Log.e("saveToExternalStorage()", e.getMessage());
                    notifyResult(false);
                } finally {
                    try {
                        if (out != null) {
                            out.close();
                        }
                    } catch (IOException e) {
                        Log.e("close FileOutput", e.getMessage());
                    }
                }
            }
        });
    }

    private void notifyResult( final boolean isSuccess) {
        Ui.run(new Runnable() {
            @Override
            public void run() {
                if (callback != null) {
                    callback.onResult(isSuccess);
                }
            }
        });
    }
}