package com.android.varun.journalentry;


import android.app.Notification;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.BroadcastReceiver;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.support.v4.app.NotificationCompat;
import android.util.Log;

public class notificationHelper extends BroadcastReceiver
{

    @Override
    public void onReceive(Context context, Intent intent) {
        showNotification(context);
    }

    private void showNotification(Context context)
    {
        PendingIntent pendingIntent = PendingIntent.getActivity
                (context, 0, new Intent(context, entryActivity.class), 0);
        Log.d("Notification Helper", "Notification Set");
        NotificationCompat.Builder builder = new NotificationCompat.Builder(context)
                .setSmallIcon(R.mipmap.ic_launcher)
                .setContentTitle("How was your day?")
                .setContentText("Write all about it");
        builder.setContentIntent(pendingIntent);
        builder.setDefaults(Notification.DEFAULT_SOUND);
        builder.setAutoCancel(true);
        NotificationManager mNotificationManager = (NotificationManager) context.getSystemService
                (Context.NOTIFICATION_SERVICE);
        mNotificationManager.notify(1, builder.build());

}
}