package com.qublish.backgroundClock;

import android.app.ActivityManager;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.IBinder;
import android.util.Log;

import com.qublish.backgroundClock.service.BackgroundClockServiceImpl;

import org.apache.cordova.CallbackContext;
import org.apache.cordova.CordovaPlugin;
import org.json.JSONArray;
import org.json.JSONException;


/**
 * This class echoes a string called from JavaScript.
 */
public class BackgroundClockPlugin extends CordovaPlugin {

    private static final String TAG = "BackgroundClock";
    private static final String INIT = "init";
    private static final String FINISH = "finish";
    private static final String GETTIME = "gettime";
    private static final String GETSTATUS = "getstatus";
    private static final String UPDATETIME = "updatetime";


    BackgroundClockServiceImpl mBoundService;
    boolean mServiceBound = false;


    @Override
    public boolean execute(String action, JSONArray args, CallbackContext callbackContext) throws JSONException {

        if (isMyServiceRunning() && mBoundService == null) {
            stopBackgroundClockService();
            callbackContext.error("mBoundService is null");
            return false;
        }

        if (action.equals(INIT)) {
            long date = args.getLong(0);
            final int interval = args.getInt(1);
            startBackgroundClockService(date, interval);
            callbackContext.success("Success");
            return true;
        } else if (action.equals(FINISH)) {
            mBoundService.finish();
            callbackContext.success("Timer Canceled");
            return true;
        } else if (action.equals(UPDATETIME)) {
            long date = args.getLong(0);
            mBoundService.updateTime(date);
            callbackContext.success("Timer Canceled");
            return true;
        } else if (action.equals(GETTIME)) {
            long currentTime = mBoundService.getTime();
            callbackContext.success(String.valueOf(currentTime));
            return true;
        } else if (action.equals(GETSTATUS)) {
            boolean currentTime = isMyServiceRunning() && mBoundService.getStatus();
            callbackContext.success(String.valueOf(currentTime));
            return true;
        }
        callbackContext.error("Unable to Find Suitable Action");
        return false;
    }

    /*
     * Start BackgroundClockService
     */
    private void startBackgroundClockService(long currentTime, int interval) {
        if (!isMyServiceRunning()) {
            Context context = this.cordova.getActivity()
                    .getApplicationContext();
            Intent backgroundClockService = new Intent(context, BackgroundClockServiceImpl.class);
            backgroundClockService.putExtra("currentTime", currentTime);
            backgroundClockService.putExtra("interval", interval);
            context.startService(backgroundClockService);
            context.bindService(backgroundClockService, mServiceConnection, Context.BIND_AUTO_CREATE);
        }
    }

    /*
     * Stop BackgroundClockService
     */
    private void stopBackgroundClockService() {
        Context context = this.cordova.getActivity()
                .getApplicationContext();
        if (mServiceBound) {
            context.unbindService(mServiceConnection);
            mServiceBound = false;
        }

        Intent backgroundClockService = new Intent(context, BackgroundClockServiceImpl.class);
        context.stopService(backgroundClockService);
    }

    private boolean isMyServiceRunning() {
        Context context = this.cordova.getActivity()
                .getApplicationContext();
        ActivityManager manager = (ActivityManager) context.getSystemService(Context.ACTIVITY_SERVICE);
        for (ActivityManager.RunningServiceInfo service : manager
                .getRunningServices(Integer.MAX_VALUE)) {
            if (BackgroundClockServiceImpl.class.getName().equals(
                    service.service.getClassName())) {
                Log.e(TAG, "BackgroundClockService is Running");
                return true;
            }
        }

        Log.e(TAG, "BackgroundClockService is not Running");
        return false;
    }

    private ServiceConnection mServiceConnection = new ServiceConnection() {

        @Override
        public void onServiceDisconnected(ComponentName name) {
            mServiceBound = false;
        }

        @Override
        public void onServiceConnected(ComponentName name, IBinder service) {
            BackgroundClockServiceImpl.ServiceBinder myBinder = (BackgroundClockServiceImpl.ServiceBinder) service;
            mBoundService = myBinder.getService();
            mServiceBound = true;
        }
    };

}