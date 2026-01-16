package it.polito.mad.appcomplete;

import android.Manifest;
import android.annotation.SuppressLint;
import android.app.IntentService;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.location.Location;
import android.location.LocationListener;
import android.location.LocationManager;

import android.util.Log;

import com.firebase.geofire.GeoFire;
import com.firebase.geofire.GeoLocation;

import com.google.android.gms.location.LocationServices;
import com.google.android.gms.tasks.OnSuccessListener;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;

import java.util.concurrent.TimeUnit;


public class ManageCurrentLocationService extends IntentService {

    private boolean terminated;
    public ManageCurrentLocationService()
    {
        super("ManageCurrentLocationService");
        terminated = false;
    }

    @SuppressLint("MissingPermission")
    @Override
    protected void onHandleIntent(Intent intent) {

/*        Log.d("TAG", "onBind: ");
        //FusedLocationProviderClient client = LocationServices.getFusedLocationProviderClient(this);

        LocationRequest request = new LocationRequest()
                .setInterval(TimeUnit.SECONDS.toMillis(1))
                .setFastestInterval(TimeUnit.SECONDS.toMillis(1))
                .setPriority(LocationRequest.PRIORITY_HIGH_ACCURACY);


        final LocationListener gpsLocationListener =new LocationListener(){

            @Override
            public void onStatusChanged(String provider, int status, Bundle extras) {
                switch (status) {
                    case LocationProvider.AVAILABLE:
                        break;
                    case LocationProvider.OUT_OF_SERVICE:
                        break;
                    case LocationProvider.TEMPORARILY_UNAVAILABLE:
                        break;
                }
            }

            @Override
            public void onProviderEnabled(String provider) {

            }

            @Override
            public void onProviderDisabled(String provider) {

            }

            @Override
            public void onLocationChanged(Location location) {

                Log.d("TAG", "onLocationChanged: " + location);
            }
        };

        final LocationListener networkLocationListener = new LocationListener(){

            @Override
            public void onStatusChanged(String provider, int status, Bundle extras){

                switch (status) {
                    case LocationProvider.AVAILABLE:
                        break;
                    case LocationProvider.OUT_OF_SERVICE:
                        break;
                    case LocationProvider.TEMPORARILY_UNAVAILABLE:
                        break;
                }
            }

            @Override
            public void onProviderEnabled(String provider) {

            }

            @Override
            public void onProviderDisabled(String provider) {

            }

            @Override
            public void onLocationChanged(Location location) {
                Log.d("TAG", "onLocationChanged: " + location);

            }
        };*/
        while (!terminated)
        {
            Location location = ((LocationManager)getSystemService(Context.LOCATION_SERVICE)).getLastKnownLocation(LocationManager.GPS_PROVIDER);
            DatabaseReference ref = FirebaseDatabase.getInstance().getReference("riders_position");
            Log.d("TAG", "location update " + location);
            if (location != null) {

                GeoFire geoFire = new GeoFire(ref);
                geoFire.setLocation(FirebaseAuth.getInstance().getUid(), new GeoLocation(location.getLatitude(), location.getLongitude()), new GeoFire.CompletionListener() {
                    @Override
                    public void onComplete(String key, DatabaseError error) {

                    }
                });
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

//        ((LocationManager)getSystemService(Context.LOCATION_SERVICE)).requestLocationUpdates(LocationManager.NETWORK_PROVIDER, (long) 0, (float)0, networkLocationListener);
//        ((LocationManager)getSystemService(Context.LOCATION_SERVICE)).requestLocationUpdates(LocationManager.GPS_PROVIDER, 0, 0, gpsLocationListener);
        /*
        LocationRequest request = new LocationRequest();
        request.setInterval(1000);
        request.setFastestInterval(500);
        request.setPriority(LocationRequest.PRIORITY_HIGH_ACCURACY);
        */

        /*client.requestLocationUpdates(request, new LocationCallback() {
            @Override
            public void onLocationResult(LocationResult locationResult) {

                //DatabaseReference ref = FirebaseDatabase.getInstance().getReference("riders_position/"+preferences.getString("Uid", " ")+"/l");
                DatabaseReference ref = FirebaseDatabase.getInstance().getReference("riders_position");
                Location location = locationResult.getLastLocation();
                Log.d("TAG", "location update " + location);
                if (location != null) {

                    GeoFire geoFire = new GeoFire(ref);
                    geoFire.setLocation(FirebaseAuth.getInstance().getUid(), new GeoLocation(location.getLatitude(), location.getLongitude()), new GeoFire.CompletionListener() {
                        @Override
                        public void onComplete(String key, DatabaseError error) {

                        }
                    });



                    //ref.child("0").setValue(location.getLatitude());
                    //ref.child("1").setValue(location.getLongitude());
                }
            }
        }, null);
*/

        //TrackerService trackerService = new TrackerService();
        //trackerService.requestLocationUpdates();

    }

    @Override
    public void onDestroy() {
        Log.d("TAG", "onDestroy: ");
        terminated = true;
        super.onDestroy();
    }

    public class TrackerService {

        @SuppressLint("MissingPermission")
        private void requestLocationUpdates() {
            Log.d("TAG", "requestLocationUpdates: ");

            while (true) {

                LocationServices.getFusedLocationProviderClient(ManageCurrentLocationService.this).getLastLocation()
                        .addOnSuccessListener(new OnSuccessListener<Location>() {
                            @Override
                            public void onSuccess(Location location) {
                                Log.d("TAG", "onSuccess: finally " + location);

                                DatabaseReference ref = FirebaseDatabase.getInstance().getReference("riders_position");

                                // Got last known location. In some rare situations this can be null.
                                if (location != null) {
                                    GeoFire geoFire = new GeoFire(ref);
                                    geoFire.setLocation(FirebaseAuth.getInstance().getUid(), new GeoLocation(location.getLatitude(), location.getLongitude()), new GeoFire.CompletionListener() {
                                        @Override
                                        public void onComplete(String key, DatabaseError error) {

                                        }
                                    });
                                }
                            }
                        });

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }


}

