package edu.uw.servicedemo;

import android.app.IntentService;
import android.content.Intent;
import android.os.Handler;
import android.util.Log;
import android.widget.Toast;

/**
 * Created by class on 5/9/16.
 */
public class CountingService extends IntentService {

    private static final String TAG = "Counting";

    private Handler handler;

    public CountingService(){
        super("CountingService");

        handler = new Handler();
    }


    private int count = 0;

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        Log.v(TAG, "Intent received");

        return super.onStartCommand(intent, flags, startId);
    }

    @Override
    protected void onHandleIntent(Intent intent) {
        Log.v(TAG, "Handling intent");

        for(count=0; count<=10; count++){

            Log.v(TAG, "Count: "+count);

            //a lot of work to show a toast on UI thread
            handler.post(new Runnable() {
                @Override
                public void run() {
                    Toast.makeText(CountingService.this, "Count: "+count, Toast.LENGTH_SHORT).show();
                }
            });

            if(count == 3)
                stopSelf();

            try{
                Thread.sleep(3000);
            }
            catch (InterruptedException e){
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void onDestroy() {
        Log.v(TAG, "Service ending");
        super.onDestroy();
    }
}