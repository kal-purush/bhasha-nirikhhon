package com.example.johannes.micloudness;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;

/**
 * Created by Johannes on 22.05.2016.
 */
public class AmpReceiver extends BroadcastReceiver {

    private iMicDataHandler handler;

    public interface iMicDataHandler {
        void handleMicData(int val);
    }

    protected void setHandler(iMicDataHandler handler) {
        this.handler = handler;
    }

    @Override
    public void onReceive(Context context, Intent intent) {
        if (intent.getAction().equals("com.example.johannes.micloudness.MIC_VAL")) {
            if (handler!=null) {
                handler.handleMicData(intent.getIntExtra("amp", 0));
            }
        }
    }
}