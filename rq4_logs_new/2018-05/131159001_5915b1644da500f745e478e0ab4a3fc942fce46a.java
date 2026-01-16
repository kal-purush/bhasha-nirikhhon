package com.arbaini.getih;

import android.Manifest;
import android.annotation.SuppressLint;
import android.app.Dialog;
import android.app.ProgressDialog;
import android.content.pm.PackageManager;
import android.location.Location;
import android.os.Looper;
import android.support.v4.app.ActivityCompat;
import android.support.v4.content.ContextCompat;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.DefaultItemAnimator;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.util.Log;
import android.view.View;
import android.view.Window;
import android.widget.AdapterView;
import android.widget.Button;
import android.widget.Toast;

import com.arbaini.getih.Model.Users;
import com.arbaini.getih.utils.RVAdapter;
import com.firebase.geofire.GeoFire;
import com.firebase.geofire.GeoLocation;
import com.firebase.geofire.GeoQuery;
import com.firebase.geofire.GeoQueryEventListener;
import com.google.android.gms.location.FusedLocationProviderClient;
import com.google.android.gms.location.LocationCallback;
import com.google.android.gms.location.LocationRequest;
import com.google.android.gms.location.LocationResult;
import com.google.android.gms.location.LocationServices;
import com.google.android.gms.location.LocationSettingsRequest;
import com.google.android.gms.location.SettingsClient;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;

import java.util.ArrayList;
import java.util.List;

import static com.google.android.gms.location.LocationServices.getFusedLocationProviderClient;

public class CariDonorActivity extends AppCompatActivity implements GeoQueryEventListener {
    private String stGolDar;
    private ProgressDialog progressDialog;
    private Dialog dialog;
    private android.widget.Spinner spinner1;
    private Button cari;
    private GeoFire geoFire;
    private GeoQuery geoQuery;

    private FirebaseAuth auth;
    private DatabaseReference databaseReference;

    RecyclerView recyclerView;
    private RecyclerView.Adapter mAdapter;
    private RecyclerView.LayoutManager mLayoutManager;
    private List<Users> recordsList = new ArrayList<>();
    String userId;

    private FusedLocationProviderClient mFusedLocationClient;
    private static final String TAG = MainActivity.class.getSimpleName();

    private static final int REQUEST_PERMISSIONS_REQUEST_CODE = 34;
    protected Location mLastLocation;
    private GeoLocation geoLocation;
    private LocationRequest mLocationRequest;

    private long UPDATE_INTERVAL = 5 * 1000;  /* 10 secs */
    private long FASTEST_INTERVAL = 2000; /* 2 sec */

    private LocationCallback mLocationcallback;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_cari_donor);
        initRecyleView();
        auth = FirebaseAuth.getInstance();
        databaseReference = FirebaseDatabase.getInstance().getReference("DonorLocation");
        userId = auth.getCurrentUser().getUid();
        geoFire =  new GeoFire(databaseReference);
        if (checkPermissions()) {

            Log.d("PERMSI", "NoodD");
        } else {

            Log.d("PERMSI", "NEED");
        }


        progressDialog = new ProgressDialog(CariDonorActivity.this);
        progressDialog.setCancelable(false);
        progressDialog.setMessage("Please wait...");
        progressDialog.setIndeterminate(true);

        dialog = new Dialog(CariDonorActivity.this);
        dialog.requestWindowFeature(Window.FEATURE_NO_TITLE);
        dialog.setContentView(R.layout.pop);
        cari = dialog.findViewById(R.id.buttonCari);
        spinner1 = dialog.findViewById(R.id.et_cari_goldarah);
        spinner1.setOnItemSelectedListener(new SpinnerSelectedListener1());
        cari.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                startLocationUpdates();
            }
        });


    }

    @Override
    public void onKeyEntered(String key, GeoLocation location) {
        Log.d("HASILGEOQ",key);
        DatabaseReference tempRef = FirebaseDatabase.getInstance().getReference("users").child(key);

        tempRef.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                Users users = dataSnapshot.getValue(Users.class);
                //Log.d("TAG",users.getEmail());
                String emails = "hikmawan1232@gmail.com";
                if(emails.equals(users.getEmail())){

                    recordsList.add(users);
                    mAdapter.notifyDataSetChanged();

                }}

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    @Override
    public void onKeyExited(String key) {

    }

    @Override
    public void onKeyMoved(String key, GeoLocation location) {

    }

    @Override
    public void onGeoQueryReady() {

    }

    @Override
    public void onGeoQueryError(DatabaseError error) {

    }




    @SuppressLint("RestrictedApi")
    protected void startLocationUpdates() {

        // Create the location request to start receiving updates
        mLocationRequest = new LocationRequest();
        mLocationRequest.setPriority(LocationRequest.PRIORITY_HIGH_ACCURACY);
        mLocationRequest.setInterval(UPDATE_INTERVAL);
        mLocationRequest.setFastestInterval(FASTEST_INTERVAL);

        // Create LocationSettingsRequest object using location request
        LocationSettingsRequest.Builder builder = new LocationSettingsRequest.Builder();
        builder.addLocationRequest(mLocationRequest);
        LocationSettingsRequest locationSettingsRequest = builder.build();

        // Check whether location settings are satisfied
        // https://developers.google.com/android/reference/com/google/android/gms/location/SettingsClient
        SettingsClient settingsClient = LocationServices.getSettingsClient(this);
        settingsClient.checkLocationSettings(locationSettingsRequest);

        // new Google API SDK v11 uses getFusedLocationProviderClient(this)


        if (ActivityCompat.checkSelfPermission(this, android.Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED && ActivityCompat.checkSelfPermission(this, android.Manifest.permission.ACCESS_COARSE_LOCATION) != PackageManager.PERMISSION_GRANTED) {
            // TODO: Consider calling
            //    ActivityCompat#requestPermissions
            // here to request the missing permissions, and then overriding
            //   public void onRequestPermissionsResult(int requestCode, String[] permissions,
            //                                          int[] grantResults)
            // to handle the case where the user grants the permission. See the documentation
            // for ActivityCompat#requestPermissions for more details.

            Toast.makeText(this, "msg", Toast.LENGTH_SHORT).show();
            return;

        }

        mLocationcallback = new LocationCallback() {
            @Override
            public void onLocationResult(LocationResult locationResult) {
                // do work here
                onLocationChanged1(locationResult.getLastLocation());
            }
        };

        getFusedLocationProviderClient(this).requestLocationUpdates(mLocationRequest, mLocationcallback,
                Looper.myLooper());
    }


    public void onLocationChanged1(Location location) {
        // New location has now been determined
        String msg = "Updated Location: " +
                Double.toString(location.getLatitude()) + "," +
                Double.toString(location.getLongitude());

        // You can now create a LatLng Object for use with maps

        final float accuracy = location.getAccuracy();


        Log.d("AKURASI", Float.toString(accuracy));

        if (accuracy < 50) {
            Toast.makeText(this, msg, Toast.LENGTH_SHORT).show();
            geoLocation = new GeoLocation(location.getLatitude(), location.getLongitude());

            geoFire.setLocation(auth.getUid(), geoLocation, new GeoFire.CompletionListener() {
                @Override
                public void onComplete(String key, DatabaseError error) {
                    Toast.makeText(getApplicationContext(),"Sukses",Toast.LENGTH_SHORT).show();


                }
            });
            stopLocUpdate1();

        }


    }

    public void stopLocUpdate1() {

        getFusedLocationProviderClient(this).removeLocationUpdates(mLocationcallback);
    }


    private boolean checkPermissions() {
        if (ContextCompat.checkSelfPermission(this,
                Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED) {
            return true;
        } else {
            requestPermissions();
            return false;
        }
    }

    private void requestPermissions() {
        String[] permissions = {
                "android.permission.ACCESS_FINE_LOCATION",
                "android.permission.ACCESS_COARSE_LOCATION",
                "android.permission.INTERNET"
        };
        int requestCode = 200;
        ActivityCompat.requestPermissions(this, permissions, requestCode);
    }


    public void setGoldarValue1(String Goldar) {


        stGolDar = Goldar;

    }
    private void cariLocation() {

        this.geoQuery = this.geoFire.queryAtLocation(geoLocation, 15);
        geoQuery.addGeoQueryEventListener(this);

    }

    public void toaster() {
        Toast.makeText(this, "null", Toast.LENGTH_SHORT).show();
    }

    private void initRecyleView(){
        recyclerView = (RecyclerView) findViewById(R.id.rv_hasilcari);
        mAdapter = new RVAdapter(recordsList);
        mLayoutManager = new LinearLayoutManager(getApplicationContext());
        recyclerView.setLayoutManager(mLayoutManager);
        recyclerView.setItemAnimator(new DefaultItemAnimator());
        recyclerView.setAdapter(mAdapter);
    }
    public class SpinnerSelectedListener1 implements AdapterView.OnItemSelectedListener {

        //get strings of first item
        String firstItem = String.valueOf(spinner1.getSelectedItem());

        public void onItemSelected(AdapterView<?> parent, View view, int pos, long id) {
            if (firstItem.equals(String.valueOf(spinner1.getSelectedItem()))) {


                setGoldarValue1(parent.getItemAtPosition(pos).toString());

               /* Toast.makeText(parent.getContext(),
                        "You have selected : " + parent.getItemAtPosition(pos).toString(),
                        Toast.LENGTH_LONG).show();
*/

            } else {

                setGoldarValue1(parent.getItemAtPosition(pos).toString());
                /*Toast.makeText(parent.getContext(),
                        "You have selected : " + parent.getItemAtPosition(pos).toString(),
                        Toast.LENGTH_LONG).show();

                */
            }
        }

        @Override
        public void onNothingSelected(AdapterView<?> arg) {

        }

    }
}