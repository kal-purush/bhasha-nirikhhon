package com.example.shane.getmeup;

import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.media.MediaPlayer;
import android.media.RingtoneManager;
import android.net.Uri;

import java.io.IOException;

/**
 * Created by Eazhilarasi on 23/11/2015.
 */
public class AlarmReceiver extends BroadcastReceiver {
    public static MediaPlayer mediaPlayer;

    public  AlarmReceiver()
    {

    }
    @Override
    public void onReceive(Context context, Intent intent) {

        String alarmType = intent.getExtras().getString("TYPE");
        if(alarmType.equals("SHAKE")) {
            PendingIntent Sender = PendingIntent.getBroadcast(context, 0, intent, 0);
            NotificationManager manager = (NotificationManager) context.getSystemService(android.content.Context.NOTIFICATION_SERVICE);
            Uri notification = RingtoneManager.getDefaultUri(RingtoneManager.TYPE_RINGTONE);

            MediaPlayer player;
            player = MediaPlayer.create(context, notification);
            player.setLooping(true);
            player.start();
            int value = intent.getExtras().getInt("NoOfShakes");

            Intent i = new Intent();
            i.putExtra("TYPE", alarmType);
            i.putExtra("NoOfShakes", value);
            i.setClassName("com.example.shane.getmeup", "com.example.shane.getmeup.PutOffAlarmActivity");
            i.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
            context.startActivity(i);
        }

        else if(alarmType.equals("TALK"))
        {

            String recordFileName = intent.getExtras().getString("FileName");
            mediaPlayer = new MediaPlayer();
            try {
                mediaPlayer.setDataSource(recordFileName);
                mediaPlayer.prepare();
            } catch (IOException e) {

            }
            mediaPlayer.setLooping(true);
            mediaPlayer.start();
            Intent i = new Intent();
            i.putExtra("TYPE", alarmType);
            i.setClassName("com.example.shane.getmeup", "com.example.shane.getmeup.PutOffAlarmActivity");
            i.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
            context.startActivity(i);
        }

    }
}