package com.garbagemon.jonas.garbagemongo;

import android.content.Context;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class Save {
    public static void saveFile(Context context, String file, byte[] data) {
        FileOutputStream fos;
        try {
            fos = context.openFileOutput(file, Context.MODE_PRIVATE);
            fos.write(data);
            fos.close();
        } catch (IOException e) {e.printStackTrace();}
    }

    public static String loadFile(Context context, String file) {
        FileInputStream fis;
        StringBuilder sb =  new StringBuilder();

        try {
            fis = context.openFileInput(file);

            InputStreamReader isr = new InputStreamReader(fis);

            BufferedReader br = new BufferedReader(isr);

            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }

            fis.close();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return sb.toString();
    }
}