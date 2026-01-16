package com.smartverse.churchlitebackend.common;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class FileCommon {

    public static String loadMod(String fileName){

        ClassLoader classLoader = FileCommon.class.getClassLoader();
        InputStream inputStream = null;
        inputStream = classLoader.getResourceAsStream(String.format("models/email/%s.mo", fileName));

        return convertInputStreamToString(inputStream);
    }

    public static String convertInputStreamToString(InputStream inputStream){
        try{
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line + "\n");
            }
            reader.close();
            return stringBuilder.toString();
        } catch (Exception ex){
            throw new RuntimeException(ex.getMessage());
        }
    }
}