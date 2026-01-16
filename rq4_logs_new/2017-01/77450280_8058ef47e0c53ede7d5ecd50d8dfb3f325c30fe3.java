package flcker.cn.greenmangotime;

import android.Manifest;
import android.app.Application;
import android.content.Context;
import android.content.pm.PackageManager;
import android.location.LocationManager;
import android.support.v4.app.ActivityCompat;

import flcker.cn.greenmangotime.location.BDLocationService;

/**
 * Created by liujiankang on 2016/12/28.
 */

public class MainApplication extends Application {
    private static MainApplication mInstance;

    // baidu location service
    public BDLocationService bdLocationService;
    LocationManager locationManager ;

    @Override
    public void onCreate() {
        super.onCreate();
        mInstance = this;
        locationManager = (LocationManager)getSystemService(Context.LOCATION_SERVICE);
        bdLocationService = new BDLocationService(getApplicationContext());
    }

    @Override
    public void onTerminate() {
        super.onTerminate();
    }

    public static  MainApplication getApplication() {
        return mInstance;
    }

    public boolean checkManifestPerssion(String permission) {
        return ActivityCompat.checkSelfPermission(this, permission) == PackageManager.PERMISSION_GRANTED;
    }

    public LocationManager getLocationManager() {
        return locationManager;
    }
}