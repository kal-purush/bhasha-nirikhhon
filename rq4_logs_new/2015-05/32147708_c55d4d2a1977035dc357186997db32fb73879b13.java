package com.webeye.photomaster.module;

import android.content.Context;
import android.util.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

/**
 * Created by sean on 5/11/15.
 */
public class JpgOptimCmdCompressor extends ICompressor {
    private final static String TAG = "JpgOptimCmdCompressor";
    private Context ctx;
    @Override
    public void init(Context ctx) {
        this.ctx = ctx;
        copyFile(ctx, "bin/jpegoptim", ctx.getApplicationInfo().dataDir + "/bin/jpegoptim");
    }

    @Override
    protected void compressJpgImpl(String source, String dest,
                                boolean openOptimizeOpt, int quality) {
        try {

            {
                Process chmodProcess = Runtime.getRuntime().exec("chmod 777 " + ctx.getApplicationInfo().dataDir + "/bin/jpegoptim");
                BufferedReader reader = new BufferedReader(new InputStreamReader(chmodProcess.getInputStream()));
                int read;
                char[] buffer = new char[4096];
                StringBuffer output = new StringBuffer();
                while ((read = reader.read(buffer)) > 0) {
                    output.append(buffer, 0, read);
                }
                reader.close();
                chmodProcess.waitFor();
                Log.i(TAG, "change mode , jpegOptim output: " + output.toString());
            }
            File dst = new File(dest);
            if(!dst.exists()) {
                dst.getParentFile().mkdirs();
                dst.createNewFile();
            }
            copy(new File(source), dst);
            {
                StringBuilder jpegOptimCmd = new StringBuilder();
                jpegOptimCmd.append(ctx.getApplicationInfo().dataDir).append("/bin/jpegoptim -m ").append(quality)
                        .append(" ").append(dest);
                Log.i(TAG, "command:" + jpegOptimCmd.toString());
                Process jpegOptimProcess = Runtime.getRuntime().exec(jpegOptimCmd.toString());
                BufferedReader reader = new BufferedReader(new InputStreamReader(jpegOptimProcess.getInputStream()));
                int read;
                char[] buffer = new char[4096];
                StringBuffer output = new StringBuffer();
                while ((read = reader.read(buffer)) > 0) {
                    output.append(buffer, 0, read);
                }
                reader.close();
                jpegOptimProcess.waitFor();
                Log.i(TAG, "jpegOptim output: " + output.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
            Log.e(TAG, "exception: " + e.getMessage());
        }

    }

    private static void copyFile(Context context, String assetPath, String localPath) {
        try {
            File file = new File(localPath);
            if(!file.exists()) {
                file.getParentFile().mkdirs();
                file.createNewFile();
            } else {
                Log.i(TAG, localPath + "already exist!!!");
                return;
            }
            InputStream in = context.getAssets().open(assetPath);
            FileOutputStream out = new FileOutputStream(localPath);
            int read;
            byte[] buffer = new byte[4096];
            while ((read = in.read(buffer)) > 0) {
                out.write(buffer, 0, read);
            }
            out.close();
            in.close();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void copy(File src, File dst) throws IOException {
        InputStream in = new FileInputStream(src);
        OutputStream out = new FileOutputStream(dst);

        // Transfer bytes from in to out
        byte[] buf = new byte[1024];
        int len;
        while ((len = in.read(buf)) > 0) {
            out.write(buf, 0, len);
        }
        in.close();
        out.close();
    }
}