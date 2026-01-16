package com.example.cmput301f20t18;

import androidx.fragment.app.FragmentActivity;

import android.os.Bundle;

import com.google.android.gms.maps.CameraUpdateFactory;
import com.google.android.gms.maps.GoogleMap;
import com.google.android.gms.maps.OnMapReadyCallback;
import com.google.android.gms.maps.SupportMapFragment;
import com.google.android.gms.maps.model.LatLng;
import com.google.android.gms.maps.model.MarkerOptions;

public class ShowMapLocationActivity extends FragmentActivity implements OnMapReadyCallback {

    private GoogleMap mMap;
    private UserLocation pickupLocation;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_show_map_location);
        // Obtain the SupportMapFragment and get notified when the map is ready to be used.
        SupportMapFragment mapFragment = (SupportMapFragment) getSupportFragmentManager()
                .findFragmentById(R.id.map);
        mapFragment.getMapAsync(this);
        pickupLocation = getPickupLocation();
    }

    private UserLocation getPickupLocation() {
        pickupLocation = new UserLocation(
                getIntent().getStringExtra("INPUT_TITLE"),
                getIntent().getDoubleExtra("INPUT_LATITUDE", 0),
                getIntent().getDoubleExtra("INPUT_LONGITUDE", 0));
        return pickupLocation;
    }

    /**
     * Manipulates the map once available.
     * This callback is triggered when the map is ready to be used.
     * This is where we can add markers or lines, add listeners or move the camera. In this case,
     * we just add a marker near Sydney, Australia.
     * If Google Play services is not installed on the device, the user will be prompted to install
     * it inside the SupportMapFragment. This method will only be triggered once the user has
     * installed Google Play services and returned to the app.
     */
    @Override
    public void onMapReady(GoogleMap googleMap) {
        mMap = googleMap;

        // Add a marker in Sydney and move the camera
        LatLng markerPosition =
                new LatLng(pickupLocation.getLatitude(), pickupLocation.getLongitude());

        mMap.addMarker(new MarkerOptions().position(markerPosition)
                .title(pickupLocation.getTitle()));

        mMap.moveCamera(CameraUpdateFactory.newLatLng(markerPosition));
    }
}