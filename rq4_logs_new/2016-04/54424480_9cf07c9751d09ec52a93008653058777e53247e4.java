package com.example.chagnoda.mapit;

import android.location.Location;
import android.location.LocationListener;
import android.os.Bundle;

/**
 * Created by David on 01/04/2016.
 */
public class MyLocationListener implements LocationListener {
    @Override
    public void onLocationChanged(Location location) {
        location.getLatitude();
        location.getLongitude();


    }

    @Override
    public void onStatusChanged(String provider, int status, Bundle extras) {

    }

    @Override
    public void onProviderEnabled(String provider) {

    }

    @Override
    public void onProviderDisabled(String provider) {

    }
}