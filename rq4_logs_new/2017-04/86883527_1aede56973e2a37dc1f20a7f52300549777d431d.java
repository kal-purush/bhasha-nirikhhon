package com.ramola.vibhav;

import android.app.NotificationManager;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.support.v4.app.NotificationCompat;
import android.util.Log;

/**
 * Created by spider on 2/4/17.
 */

public class Rfidreceiver extends BroadcastReceiver {

    public static final String RFidrecrive ="Receive";
   public static final String Rfid="qwer1234";
    private static final int UPLOAD_ID=11126741;

    public void onReceive(Context context, Intent intent)
    {
        NotificationManager notificationManager=(NotificationManager) context.getSystemService(Context.NOTIFICATION_SERVICE);
        NotificationCompat.Builder builder=new NotificationCompat.Builder(context);
        switch (intent.getAction())
        {
            case RFidrecrive:
                notificationManager.cancel(UPLOAD_ID);
                Log.d("Recievie","Rfid Recieved");
                builder.setContentTitle("The Given RFID IS RECIEVED  "+intent.getStringExtra(Rfid));
                builder.setSmallIcon(R.drawable.female);
                notificationManager.notify(UPLOAD_ID,builder.build());
                break;
        }
    }
}