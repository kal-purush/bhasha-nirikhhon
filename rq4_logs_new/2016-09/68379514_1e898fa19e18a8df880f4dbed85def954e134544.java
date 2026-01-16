package com.andres.wherearemyuser;

import android.app.Activity;
import android.content.Context;
import android.location.Location;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.widget.Toast;

import com.google.android.gms.common.ConnectionResult;
import com.google.android.gms.common.api.GoogleApiClient;
import com.google.android.gms.location.LocationListener;
import com.google.android.gms.location.LocationRequest;
import com.google.android.gms.location.LocationServices;


public class LocationManager implements GoogleApiClient.ConnectionCallbacks, GoogleApiClient.OnConnectionFailedListener, LocationListener {

    public static final int INTERVAL_IN_MILLIS = 30000;
    public static final int FASTEST_INTERVAL_IN_MILLIS = 10000;
    public static final float MINIMUM_DISTANCE_CHANGE = 20;

    private static LocationManager mLocationManager;

    private Context mcontext;
    private Activity mActivity;

    private GoogleApiClient mGoogleApiClient;
    private LocationRequest mLocationRequest;
    private Location mLastLocation;

    public static LocationManager getInstance(Activity activity) {
        if (mLocationManager == null) {
            mLocationManager = new LocationManager(activity);
        }
        return mLocationManager;
    }

    private LocationManager(Activity activity) {
        this.mcontext = activity.getApplicationContext();
        this.mActivity =activity;
    }

    public synchronized void init() {
        createLocationRequest();
        mGoogleApiClient = new GoogleApiClient.Builder(mcontext)
                .addConnectionCallbacks(this)
                .addOnConnectionFailedListener(this)
                .addApi(LocationServices.API)
                .build();
        mGoogleApiClient.connect();
    }

    private void createLocationRequest() {
        mLocationRequest = new LocationRequest();
        mLocationRequest.setInterval(INTERVAL_IN_MILLIS);
        mLocationRequest.setSmallestDisplacement(MINIMUM_DISTANCE_CHANGE);
        mLocationRequest.setFastestInterval(FASTEST_INTERVAL_IN_MILLIS);
        mLocationRequest.setPriority(LocationRequest.PRIORITY_HIGH_ACCURACY);
    }

    private void startLocationUpdates() {
        if (isConnected()) {
            LocationServices.FusedLocationApi.requestLocationUpdates(mGoogleApiClient, mLocationRequest, this);
        }
    }

    private void stopLocationUpdates() {
        if (isConnected()) {
            LocationServices.FusedLocationApi.removeLocationUpdates(mGoogleApiClient, this);
            disconnectGoogleApiClient();
        }
    }

    @Override
    public void onConnected(Bundle connectionHint) {
        if (isConnected()) {
            mLastLocation = LocationServices.FusedLocationApi.getLastLocation(mGoogleApiClient);
            if (mLastLocation != null) {
                saveLocation(mLastLocation);
                stopLocationUpdates();
            } else {
                startLocationUpdates();
            }
        }
    }

    @Override
    public void onLocationChanged(Location location) {
        mLastLocation = location;
        saveLocation(mLastLocation);
        stopLocationUpdates();
    }

    public void saveLocation(Location lastLocation){
        new Utils().saveMylastlocation(mActivity,lastLocation);
    }

    @Override
    public void onConnectionFailed(@NonNull ConnectionResult connectionResult) {
        connectGoogleApiClient();
    }

    @Override
    public void onConnectionSuspended(int i) {
        connectGoogleApiClient();
    }

    private void connectGoogleApiClient() {
        if (!isConnected()) {
            mGoogleApiClient.connect();
        }
    }

    private boolean isConnected() {
        return mGoogleApiClient != null && mGoogleApiClient.isConnected();
    }

    private void disconnectGoogleApiClient() {
        if (isConnected()) {
            mGoogleApiClient.disconnect();
        }
    }
}