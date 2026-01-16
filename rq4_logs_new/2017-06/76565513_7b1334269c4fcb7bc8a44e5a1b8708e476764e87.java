package com.devsuda.lancasterprayertimes;

import android.app.Notification;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.net.Uri;

/**
 * Created by Abubakr on 12/06/2017.
 */

public class NotiAdaptor {

    private Context ctx;
    public NotiAdaptor(MainActivity _mainActivity){
        this.ctx = _mainActivity;
    }

    public void showNotification(int notiId) {
        Notification myNotification = null;
        String notificationTitle = null;
        String notificationBody = null;

        switch (notiId) {
            case 1:
                myNotification = new Notification(R.drawable.ic_launcher, "Time for praying!", System.currentTimeMillis());
                notificationTitle = "Time for praying";
                notificationBody = "Prepare yourself!";

                break;
            case 2:
                myNotification = new Notification(R.drawable.ic_launcher, "Jamaa started!", System.currentTimeMillis());
                notificationTitle = "Jamaa started!";
                notificationBody = "May Allah accept";
                break;
        }
        NotificationManager notificationManager = (NotificationManager) ctx.getSystemService(Context.NOTIFICATION_SERVICE);

        //Context context = ctx.getApplicationContext();


        Intent myIntent = new Intent(ctx, MainActivity.class);
        PendingIntent pendingIntent = PendingIntent.getActivity(ctx, 0, myIntent, Intent.FILL_IN_ACTION);
        myNotification.flags |= Notification.FLAG_AUTO_CANCEL;
        myNotification.sound = Uri.parse("android.resource://" + ctx.getPackageName() + "/" + R.raw.azan);
        myNotification.setLatestEventInfo(ctx, notificationTitle, notificationBody, pendingIntent);
        notificationManager.notify(1, myNotification);
    }
}