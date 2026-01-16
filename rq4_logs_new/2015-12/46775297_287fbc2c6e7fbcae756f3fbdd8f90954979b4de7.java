package com.group33.greenthumb;

/**
 * Created by dzhang on 12/1/15.
 */
import android.content.Intent;

import com.google.android.gms.wearable.MessageEvent;
import com.google.android.gms.wearable.WearableListenerService;


public class WatchListenerService extends WearableListenerService {

    private static final String START_ACTIVITY = "/start_activity";

    @Override
    public void onMessageReceived(MessageEvent messageEvent) {

        if( messageEvent.getPath().equalsIgnoreCase( START_ACTIVITY ) ) {

            Intent it = new Intent(getBaseContext(), WatchHome.class);
            it.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);

            startActivity(it);

        } else {
        }

    }

}