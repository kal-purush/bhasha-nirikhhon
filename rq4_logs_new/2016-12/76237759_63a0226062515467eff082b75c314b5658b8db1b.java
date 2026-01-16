package com.example.finalproject;

import android.app.Activity;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.util.Log;

import static android.content.Intent.ACTION_SCREEN_OFF;
import static android.content.Intent.ACTION_SCREEN_ON;

/**
 * Created by Shower on 2016/12/18 0018.
 */

public class DynamicReceiver extends BroadcastReceiver {
    private DataInteraction dataInteraction;

    @Override
    public void onReceive(Context context, Intent intent) {
        if (intent.getAction().equals(ACTION_SCREEN_OFF)) {
            dataInteraction.setIsLock(true);
        } else if (intent.getAction().equals(ACTION_SCREEN_ON)) {
            dataInteraction.setIsLock(false);
        }
    }

    public interface DataInteraction {
        void setIsLock(boolean flag);
    }

    public void setDataListener(DataInteraction dataInteraction) {
        this.dataInteraction = dataInteraction;
    }
}