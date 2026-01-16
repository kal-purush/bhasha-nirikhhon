package com.android.lvxin.sdk.app;

import android.app.Application;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;
import android.widget.Toast;

import com.android.lvxin.sdk.service.AppService;

/**
 * @ClassName: com.android.lvxin.sdk.app.BaseApp
 * @Description: TODO
 * @Author: lvxin
 * @Date: 11/26/15 16:08
 */
public class BaseApp extends Application {

    private static BaseApp instance;

    private final Handler APP_HANDLER = new Handler();

    private final ServiceConnection serviceConnection = new ServiceConnection() {
        @Override
        public void onServiceConnected(ComponentName name, IBinder service) {
            //TODO: Do something
        }

        @Override
        public void onServiceDisconnected(ComponentName name) {

        }
    };
    private Toast mToast;

    private void setInstance(BaseApp instance) {
        this.instance = instance;
    }

    /**
     * get the object of the BaseApp
     *
     * @return this
     */
    public BaseApp getApp() {
        return instance;
    }

    public Handler getAppHandler() {
        return APP_HANDLER;
    }

    @Override
    public void onCreate() {
        setInstance(this);
        super.onCreate();
        startService();
        initCommon();
    }

    /**
     * start service and bind with application
     */
    private void startService() {
        Intent intent = new Intent(getApplicationContext(), AppService.class);
        startService(intent);
        bindService(intent, serviceConnection, Context.BIND_AUTO_CREATE);
    }

    /**
     * initialize the common data
     */
    private void initCommon() {

        //TODO: init dir, init screen size, device info, etc.
    }

    public void showToast(int contentId) {
        showToast(getString(contentId));
    }

    public void showToast(final String content) {
        if (Looper.getMainLooper() != Looper.myLooper()) {
            // do this in the main thread
            getAppHandler().post(new Runnable() {
                @Override
                public void run() {
                    doShowToast(content);
                }
            });
        } else {
            doShowToast(content);
        }
    }

    private void doShowToast(String content) {
        if (null == mToast) {
            mToast = Toast.makeText(this, content, Toast.LENGTH_SHORT);
        } else {
            mToast.setText(content);
        }

        mToast.show();
    }
}