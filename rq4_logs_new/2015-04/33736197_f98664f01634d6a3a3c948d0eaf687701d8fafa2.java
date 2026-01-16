package neamar.fr.thequietmind.services;

import android.content.ContentValues;
import android.database.sqlite.SQLiteDatabase;
import android.service.notification.NotificationListenerService;
import android.service.notification.StatusBarNotification;
import android.util.Log;

import java.util.Date;

import neamar.fr.thequietmind.db.NotificationDisplayedContract;
import neamar.fr.thequietmind.db.NotificationDisplayedDbHelper;

public class NotificationListener extends NotificationListenerService {

    private String TAG = this.getClass().getSimpleName();
    private NotificationDisplayedDbHelper notificationDisplayedDbHelper;
    private SQLiteDatabase db;

    @Override
    public void onCreate() {
        super.onCreate();
        Log.w(TAG, "**********  onCreate()");
        notificationDisplayedDbHelper = new NotificationDisplayedDbHelper(this);
        db = notificationDisplayedDbHelper.getWritableDatabase();
    }

    @Override
    public void onNotificationPosted(StatusBarNotification sbn) {
        Log.w(TAG, "**********  onNotificationPosted");
        Log.w(TAG, "ID :" + sbn.getId() + "\t" + sbn.getNotification().tickerText + "\t" + sbn.getPackageName());

        // Create a new map of values, where column names are the keys
        Date now = new Date();
        ContentValues values = new ContentValues();
        values.put(NotificationDisplayedContract.NotificationDisplayedEntry.COLUMN_NAME_PACKAGE, sbn.getPackageName());
        values.put(NotificationDisplayedContract.NotificationDisplayedEntry.COLUMN_NAME_DATE, now.getTime());
        values.put(NotificationDisplayedContract.NotificationDisplayedEntry.COLUMN_NAME_HOUR, now.getHours());
        values.put(NotificationDisplayedContract.NotificationDisplayedEntry.COLUMN_NAME_DATE, now.getDay());

        // Insert the new row, returning the primary key value of the new row
        db.insertOrThrow(
                NotificationDisplayedContract.NotificationDisplayedEntry.TABLE_NAME,
                null,
                values);
    }

    @Override
    public void onNotificationRemoved(StatusBarNotification sbn) {
        Log.w(TAG, "********** onNotificationRemoved");
        Log.w(TAG, "ID :" + sbn.getId() + "\t" + sbn.getNotification().tickerText + "\t" + sbn.getPackageName());
    }
}