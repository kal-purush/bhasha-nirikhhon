package com.keepsolid.wetalkas.keepsolid.services;

import android.app.AlarmManager;
import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.util.Log;

import java.util.Calendar;

/**
 * Created by wetalkas on 03.08.15.
 */
public class AlarmManagerHelper {
    private AlarmManager alarmManager;


    private static AlarmManagerHelper instance;

    private Context context;


    private AlarmManagerHelper() {

    }



    public static AlarmManagerHelper getInstance() {

        if (instance == null) {
            instance = new AlarmManagerHelper();
        }

        return instance;
    }


    public void init(Context context){
        this.context = context;
        this.alarmManager = (AlarmManager) context.getSystemService(
                Context.ALARM_SERVICE);
    }




    public void setNotification(String taskTitle, long taskDate, long requestCode) {

        Intent intent = new Intent(context,
                TaskReminderService.class);

        intent.setAction(requestCode + "");


        intent.putExtra("task_title", taskTitle);
        intent.putExtra("task_time", requestCode);

        PendingIntent pendingIntent = PendingIntent.getService(context, (int) requestCode,
                intent, 0);

        Log.d("requestCode", "set = " + requestCode);

        //getActivity().startService(intent);

        /*AlarmManager manager = (AlarmManager) context.getSystemService(
                Context.ALARM_SERVICE);*/

        alarmManager.set(AlarmManager.RTC_WAKEUP, taskDate, pendingIntent);

    }

    public void cancelNotification(String taskTitle, long requestCode) {
        Intent intent = new Intent(context,
                TaskReminderService.class);

        intent.setAction(requestCode + "");


        intent.putExtra("task_title", taskTitle);
        intent.putExtra("task_time", requestCode);

        PendingIntent pendingIntent = PendingIntent.getService(context, (int) requestCode,
                intent, 0);

        Log.d("requestCode", "cancel = " + requestCode);


        alarmManager.cancel(pendingIntent);
    }

    public AlarmManager getAlarmManager() {
        return alarmManager;
    }
}