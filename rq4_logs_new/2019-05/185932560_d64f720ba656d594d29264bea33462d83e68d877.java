package com.sun.music_64.service;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.TaskStackBuilder;
import android.content.Context;
import android.content.Intent;
import android.os.Build;
import android.support.v4.app.NotificationCompat;
import android.widget.RemoteViews;

import com.sun.music_64.R;
import com.sun.music_64.data.model.Track;
import com.sun.music_64.screen.MainActivity;

public class NotificationMusic {
    public static final int NOTIFICATION_ID = 1000;
    private static final int REQUEST_CODE = 1;
    private static final String CHANNEL_NAME = "com.sun.music_64.CHANNEL_NAME";
    private static final String CHANNEL_ID = "com.sun.music_64.CHANNEL_ID";
    private RemoteViews mRemoteViews;
    private NotificationCompat.Builder mBuilder;
    private NotificationManager mManager;

    public Notification initLayoutNotification(Context context, Track track) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel channel = new NotificationChannel(CHANNEL_ID,
                    CHANNEL_NAME, NotificationManager.IMPORTANCE_LOW);
            NotificationManager notificationManager = context.getSystemService(
                    NotificationManager.class
            );
            notificationManager.createNotificationChannel(channel);
        }

        Intent detailIntent = new Intent(context, MainActivity.class);

        //use taskstackbuilder to navigate MainActivity
        TaskStackBuilder stackBuilder = TaskStackBuilder.create(context);
        stackBuilder.addNextIntentWithParentStack(detailIntent);

        PendingIntent pendingIntent = stackBuilder.getPendingIntent(
                REQUEST_CODE, PendingIntent.FLAG_UPDATE_CURRENT);
        mBuilder = new NotificationCompat.Builder(context, CHANNEL_ID);
        mBuilder.setSmallIcon(R.drawable.icon_app)
                .setContentTitle(context.getString(R.string.app_name))
                .setCustomContentView(mRemoteViews)
                .setContentIntent(pendingIntent)
                .setPriority(NotificationCompat.PRIORITY_LOW);
        mManager = (NotificationManager) context.getSystemService(context.NOTIFICATION_SERVICE);
        initViews(context, track);
        Notification notification = mBuilder.build();
        clickPreviousButton(context);
        clickPlayButton(context);
        clickNextButton(context);
        return notification;
    }

    public void clickNextButton(Context context) {
        Intent intent = new Intent(context, PlayMusicService.class);
        intent.setAction(PlayMusicService.ACTION_NEXT);
        PendingIntent pendingIntent =
                PendingIntent.getService(context, REQUEST_CODE, intent, 0);
        mRemoteViews.setOnClickPendingIntent(R.id.image_next_notifi, pendingIntent);
    }

    public void clickPlayButton(Context context) {
        Intent intent = new Intent(context, PlayMusicService.class);
        intent.setAction(PlayMusicService.ACTION_PLAY_AND_PAUSE);
        PendingIntent pendingIntent =
                PendingIntent.getService(context, REQUEST_CODE, intent, 0);
        mRemoteViews.setOnClickPendingIntent(R.id.image_play_notifi, pendingIntent);
    }

    public void clickPreviousButton(Context context) {
        Intent intent = new Intent(context, PlayMusicService.class);
        intent.setAction(PlayMusicService.ACTION_PREVIOUS);
        PendingIntent pendingIntent =
                PendingIntent.getService(context, REQUEST_CODE, intent, 0);
        mRemoteViews.setOnClickPendingIntent(R.id.image_previous_notifi, pendingIntent);
    }

    public void initViews(Context context, Track track) {
        mRemoteViews = new RemoteViews(context.getPackageName(),
                R.layout.layout_notification);
        mRemoteViews.setTextViewText(R.id.text_name_song, track.getTitle());
        mRemoteViews.setTextViewText(R.id.text_song_artist, track.getArtist());
    }

    public void updatePlayAndPauseState(boolean isPlaying) {
        if (isPlaying) {
            mRemoteViews.setImageViewResource(R.id.image_play_notifi,
                    R.drawable.ic_pause_black_24dp);
        } else {
            mRemoteViews.setImageViewResource(R.id.image_play_notifi,
                    R.drawable.ic_play_arrow_black_24dp);
        }
        mManager.notify(NOTIFICATION_ID, mBuilder.build());
    }
}