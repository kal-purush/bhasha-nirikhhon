package com.russell.calmmusic.tools.datas;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * Created by Russell on 16/2/2.
 */
public class NetEaseLyric {
    File file = null;
    public void byteReadFile(String fileName){
        InputStream inputStream = null;
        file = new File(fileName);
        try {
            inputStream = new FileInputStream(file);
            int tempbyte;
            while ((tempbyte = inputStream.read()) != -1) {
                System.out.write(tempbyte);
            }
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String charReadFile(String fileName) {
        file = new File(fileName);
        Reader reader = null;
        String temp = "";
        try {
            reader = new InputStreamReader(new FileInputStream(file));
            int tempChar;
            while ((tempChar = reader.read()) != -1) {
                if (((char) tempChar) != '\r') {
                    temp += (char)tempChar;
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return temp;
    }

    public String outLyric(String fileName){
        String s = charReadFile(fileName);
        String lyric = "";
        try {
            JSONObject jsonObject = new JSONObject(s);
            lyric = jsonObject.getString("lyric");
            System.out.print(lyric);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return lyric;
    }
}