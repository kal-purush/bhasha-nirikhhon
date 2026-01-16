package com.example.meizu.supporttestclilent;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;

/**
 * Created by meizu on 2016/7/2.
 */
public class AlarmReceiver extends BroadcastReceiver{
    @Override
    public void onReceive(Context context, Intent intent) {
        Intent i = new Intent(context, LongRunService.class);
        context.startService(i);
    }
}