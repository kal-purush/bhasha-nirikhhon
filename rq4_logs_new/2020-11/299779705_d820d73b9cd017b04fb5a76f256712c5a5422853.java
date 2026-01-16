package com.example.cmput301f20t18;

import android.os.Build;

import androidx.annotation.RequiresApi;

import java.util.Base64;

public class byteStringExchange {


    /**
     * Converts a byte representing a photo to a String
     * @param photo The byte[] object representation of a photo
     */
    @RequiresApi(api = Build.VERSION_CODES.O)
    public String byteToString(byte[] photo){
        return Base64
                .getEncoder()
                .encodeToString(photo);
    }

    /**
     * Converts a String representing a photo to a byte[]
     * @param photo The String object representation of a photo
     */
    @RequiresApi(api = Build.VERSION_CODES.O)
    public byte[] stringToByte(String photo){
        return Base64.getDecoder().decode(photo);
    }
}