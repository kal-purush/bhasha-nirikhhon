package jlarison.multimeterreader;

import android.location.Location;
import android.os.Handler;
import android.os.Message;
import android.support.v4.app.FragmentActivity;
import android.os.Bundle;
import android.view.View;

import com.google.android.gms.common.ConnectionResult;
import com.google.android.gms.common.api.GoogleApiClient;
import com.google.android.gms.location.LocationListener;
import com.google.android.gms.location.LocationRequest;
import com.google.android.gms.location.LocationServices;
import com.google.android.gms.maps.CameraUpdateFactory;
import com.google.android.gms.maps.GoogleMap;
import com.google.android.gms.maps.SupportMapFragment;
import com.google.android.gms.maps.model.LatLng;
import com.google.android.gms.maps.model.Marker;
import com.google.android.gms.maps.model.MarkerOptions;
import com.google.android.gms.common.api.GoogleApiClient.ConnectionCallbacks;
import com.google.android.gms.common.api.GoogleApiClient.OnConnectionFailedListener;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MapsActivity extends FragmentActivity implements ConnectionCallbacks, OnConnectionFailedListener{

    private GoogleMap mMap; // Might be null if Google Play services APK is not available.
    private GoogleApiClient mGoogleApiClient;
    private Location mLastLocation;
    public Handler handler;
    private LocationRequest mLocationRequest;
    private LocationListener mLocationListener;


    @Override
    public void onConnected(Bundle bundle) {
        mLastLocation = LocationServices.FusedLocationApi.getLastLocation(mGoogleApiClient);
        //startLocationUpdates();
    }

    @Override
    protected void onStart() {
        super.onStart();
        mGoogleApiClient.connect();
    }

    @Override
    protected void onStop() {
        super.onStop();
        if (mGoogleApiClient.isConnected()) {
            mGoogleApiClient.disconnect();
        }
    }

    @Override
    public void onConnectionSuspended(int i) {
        // The connection to Google Play services was lost for some reason. We call connect() to
        // attempt to re-establish the connection.
        // Log.i(TAG, "Connection suspended");
        mGoogleApiClient.connect();
    }

    @Override
    public void onConnectionFailed(ConnectionResult connectionResult) {

    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_maps);
        setUpMapIfNeeded();
        buildGoogleApiClient();


        handler = new Handler(){
            @Override
            public void handleMessage(android.os.Message message){
                try {
                    android.location.Location loc = (android.location.Location) message.obj;
                    placeMarker(loc.getLatitude(), loc.getLongitude());
                }catch(Exception e){

                }
            }
        };

        Thread T = new loc(mGoogleApiClient, this);
        T.start();

    }

    protected synchronized void buildGoogleApiClient() {
        mGoogleApiClient = new GoogleApiClient.Builder(this)
                .addConnectionCallbacks(this)
                .addOnConnectionFailedListener(this)
                .addApi(LocationServices.API)
                .build();
    }

    protected void startLocationUpdates() {
        LocationServices.FusedLocationApi.requestLocationUpdates(
                mGoogleApiClient, mLocationRequest, mLocationListener);
    }

    @Override
    protected void onResume() {
        super.onResume();
        setUpMapIfNeeded();
    }

    /**
     * Sets up the map if it is possible to do so (i.e., the Google Play services APK is correctly
     * installed) and the map has not already been instantiated.. This will ensure that we only ever
     * call {@link #setUpMap()} once when {@link #mMap} is not null.
     * <p/>
     * If it isn't installed {@link SupportMapFragment} (and
     * {@link com.google.android.gms.maps.MapView MapView}) will show a prompt for the user to
     * install/update the Google Play services APK on their device.
     * <p/>
     * A user can return to this FragmentActivity after following the prompt and correctly
     * installing/updating/enabling the Google Play services. Since the FragmentActivity may not
     * have been completely destroyed during this process (it is likely that it would only be
     * stopped or paused), {@link #onCreate(Bundle)} may not be called again so we should call this
     * method in {@link #onResume()} to guarantee that it will be called.
     */
    private void setUpMapIfNeeded() {
        // Do a null check to confirm that we have not already instantiated the map.
        if (mMap == null) {
            // Try to obtain the map from the SupportMapFragment.
            mMap = ((SupportMapFragment) getSupportFragmentManager().findFragmentById(R.id.map))
                    .getMap();
            // Check if we were successful in obtaining the map.
            if (mMap != null) {
                setUpMap();
            }
        }
    }


    /**
     * This is where we can add markers or lines, add listeners or move the camera. In this case, we
     * just add a marker near Africa.
     * <p/>
     * This should only be called once and when we are sure that {@link #mMap} is not null.
     */
    private void setUpMap() {
        Marker m;
        mMap.setMyLocationEnabled(true);
        //mMap.setTrafficEnabled(true);
        //mMap.setLocationSource();
        mMap.moveCamera(CameraUpdateFactory.newLatLngZoom(new LatLng(38.8833, -98.3500), 2.9f));
        m = mMap.addMarker(new MarkerOptions().position(new LatLng(40.552586, -105.077100)).title("Fort Collins").snippet("Intersection of Drake Rd and College Ave"));
        //m.showInfoWindow();
    }

    private void placeMarker(double lat, double longi) {
        DateFormat df = new SimpleDateFormat("HH:mm:ss");
        Date dateobj = new Date();
        mMap.addMarker(new MarkerOptions().position(new LatLng(lat, longi)).title(df.format(dateobj)).snippet(new LatLng(lat, longi).toString()));
    }

    public void joshlarison(View view) {
        mMap.clear();
    }

    public void toggleTraffic(View view) {
        if(mMap.isTrafficEnabled()){
            mMap.setTrafficEnabled(false);
        }else{
            mMap.setTrafficEnabled(true);
        }
    }
}

class loc extends Thread{
    GoogleApiClient apiClient;
    Location mlLastLocation;
    MapsActivity MA;

    public loc(GoogleApiClient apiClient, MapsActivity MA){
        this.apiClient = apiClient;
        this.MA = MA;
        //buildGoogleApiClient();
    }

    @Override
    public void run(){
        //buildGoogleApiClient();
        //apiClient.connect();
        while(true) {
            mlLastLocation = LocationServices.FusedLocationApi.getLastLocation(
                    apiClient);
            //mlLastLocation.getLatitude();
            //mlLastLocation.getLongitude();
            Message m = new Message();
            m.obj = mlLastLocation;
            MA.handler.sendMessage(m);
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}