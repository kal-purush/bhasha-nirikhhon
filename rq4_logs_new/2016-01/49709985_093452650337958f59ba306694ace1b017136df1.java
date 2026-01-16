package com.heytz.ble;

import android.app.Application;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.IBinder;
import com.heytz.ble.sdk.BleService;
import com.heytz.ble.sdk.IBle;

/**
 * Created by chendongdong on 16/1/16.
 */
public class BleApplication extends Application {
    private BleService mService;
    private IBle mBle;

    private final ServiceConnection mServiceConnection = new ServiceConnection() {
        @Override
        public void onServiceConnected(ComponentName className,
                                       IBinder rawBinder) {
            mService = ((BleService.LocalBinder) rawBinder).getService();
            mBle = mService.getBle();
            if (mBle != null && !mBle.adapterEnabled()) {
                // TODO: enalbe adapter
            }
        }

        @Override
        public void onServiceDisconnected(ComponentName classname) {
            mService = null;
        }
    };
    @Override
    public void onCreate() {
        super.onCreate();

        Intent bindIntent = new Intent(this, BleService.class);
        bindService(bindIntent, mServiceConnection, Context.BIND_AUTO_CREATE);
    }

    public IBle getIBle() {
        return mBle;
    }
}