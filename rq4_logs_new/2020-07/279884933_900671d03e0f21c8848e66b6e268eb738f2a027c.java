package jp.ac.titech.itpro.sdl.startservice;

import android.app.IntentService;
import android.content.Intent;
import android.util.Log;

public class Service3 extends IntentService {
    private final static String TAG=Service3.class.getSimpleName();
    public final static String EXTRA_MYARG = "MYARG";
    private final static int MSG_TAG=1234;

    public  Service3() {
        super(TAG);
    }

    protected void onHandleIntent(Intent intent) {
        Log.d(TAG,"onHandleIntent in"+Thread.currentThread());
        Log.d(TAG,"myarg = "+intent.getStringExtra(EXTRA_MYARG));
        Intent broadcastIntent=new Intent(EXTRA_MYARG);
        broadcastIntent.putExtra(EXTRA_MYARG,"Answer to Service3");
        sendBroadcast(broadcastIntent);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void onCreate() {
        super.onCreate();
        Log.d(TAG,"onCreate in "+Thread.currentThread());
    }

    public void onDestroy() {
        Log.d(TAG,"onDestroy in "+Thread.currentThread());
        super.onDestroy();
    }
}