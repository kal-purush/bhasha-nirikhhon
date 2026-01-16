package com.applications.philipp.apkinson.services;

import android.Manifest;
import android.app.Activity;
import android.content.Context;
import android.content.pm.PackageManager;
import android.support.v4.app.ActivityCompat;
import android.support.v4.content.ContextCompat;

/**
 * Created by TOMAS on 10/11/2016.
 */

public class RequestPermission {

    //Request for permission Android >= 6
    private static final int REQUEST_PERMISSIONS = 0;

    public int RequestPermission(Context context) {
        //----------------------------------------------------------------
        //Request permission to RECORD AUDIO and STORE DATA on the phone
        //----------------------------------------------------------------
        //The app had permission to record audio?
        int audio_perm = ContextCompat.checkSelfPermission(context, Manifest.permission.RECORD_AUDIO);
        //int storage_per = ContextCompat.checkSelfPermission(this, Manifest.permission.WRITE_EXTERNAL_STORAGE);
        if (audio_perm != PackageManager.PERMISSION_GRANTED) {
            //If there is not permission to record and store audio files, then ask to the user for it
            ActivityCompat.requestPermissions((Activity) context,
                    new String[]{Manifest.permission.RECORD_AUDIO, Manifest.permission.WRITE_EXTERNAL_STORAGE,
                            Manifest.permission.MODIFY_AUDIO_SETTINGS,Manifest.permission.READ_CONTACTS,
                            Manifest.permission.READ_PHONE_STATE}, REQUEST_PERMISSIONS);
        }
        return audio_perm;
    }//EN REQUEST PERMISSION
}