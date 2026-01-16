package com.fiuba.tdpii.correapp.activities;

import android.Manifest;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.graphics.Color;
import android.location.Address;
import android.location.Geocoder;
import android.location.Location;
import android.os.Handler;
import android.support.v4.app.ActivityCompat;
import android.support.v4.app.FragmentActivity;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.widget.Button;
import android.widget.Toast;

import com.fiuba.tdpii.correapp.R;
import com.fiuba.tdpii.correapp.models.web.Destination;
import com.fiuba.tdpii.correapp.models.web.SerializedTripPostResponse;
import com.fiuba.tdpii.correapp.models.web.StartTripPutRequest;
import com.fiuba.tdpii.correapp.services.trips.TripService;
import com.google.android.gms.location.FusedLocationProviderClient;
import com.google.android.gms.location.LocationServices;
import com.google.android.gms.maps.CameraUpdateFactory;
import com.google.android.gms.maps.GoogleMap;
import com.google.android.gms.maps.OnMapReadyCallback;
import com.google.android.gms.maps.SupportMapFragment;
import com.google.android.gms.maps.model.BitmapDescriptorFactory;
import com.google.android.gms.maps.model.JointType;
import com.google.android.gms.maps.model.LatLng;
import com.google.android.gms.maps.model.LatLngBounds;
import com.google.android.gms.maps.model.MapStyleOptions;
import com.google.android.gms.maps.model.Marker;
import com.google.android.gms.maps.model.MarkerOptions;
import com.google.android.gms.maps.model.PolylineOptions;
import com.google.android.gms.tasks.OnSuccessListener;
import com.google.android.gms.tasks.Task;
import com.google.maps.DirectionsApi;
import com.google.maps.DirectionsApiRequest;
import com.google.maps.GeoApiContext;
import com.google.maps.model.DirectionsLeg;
import com.google.maps.model.DirectionsResult;
import com.google.maps.model.DirectionsRoute;
import com.google.maps.model.DirectionsStep;
import com.google.maps.model.EncodedPolyline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class StartTripDriverActivity extends FragmentActivity implements OnMapReadyCallback {

    private LatLng originLocation;
    private LatLng destinynLocation;
    private Location currentLocation;

    private FusedLocationProviderClient fusedLocationProviderClient;
    public static final int LOCATION_REQUEST_CODE = 101;
    private static final int ZOOM = 16;
    public static final int CUSTOM_MARKER_WIDTH = 100;
    public static final int CUSTOM_MARKER_HEIGHT = 100;
    private GoogleMap mMap;

    private Bundle bundle;
    private Long tripId;
    private Long driverId;

    private TripService tripService;

    private Button start;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_start_trip_driver);
        tripService = new TripService();

        bundle = getIntent().getParcelableExtra("bundle");

        tripId = bundle.getLong("tripId");
        driverId = bundle.getLong("driverId");


        start = findViewById(R.id.empezar);
        start.setOnClickListener(v -> {

            StartTripPutRequest request = new StartTripPutRequest();
            request.setStatus("started");
            tripService.startTrip(request, tripId.toString() ).enqueue(new Callback<SerializedTripPostResponse>() {
                @Override
                public void onResponse(Call<SerializedTripPostResponse> call, Response<SerializedTripPostResponse> response) {

                    response.body();

                    bundle.putLong("tripId",tripId );
                    bundle.putLong("driverId",driverId );

                    Intent navigationIntent = new Intent(StartTripDriverActivity.this, ChoferSeguimiento.class);

                    navigationIntent.putExtra("bundle", bundle );
                    startActivity(navigationIntent);
                }

                @Override
                public void onFailure(Call<SerializedTripPostResponse> call, Throwable t) {

                }
            });



        });

        fusedLocationProviderClient = LocationServices.getFusedLocationProviderClient(this);
        if (ActivityCompat.checkSelfPermission(StartTripDriverActivity.this, android.Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED && ActivityCompat.checkSelfPermission(StartTripDriverActivity.this, android.Manifest.permission.ACCESS_COARSE_LOCATION) != PackageManager.PERMISSION_GRANTED) {
            ActivityCompat.requestPermissions(this, new String[]{android.Manifest.permission.ACCESS_FINE_LOCATION}, LOCATION_REQUEST_CODE);
            return;
        }
        fetchLastLocation();

        bundle = getIntent().getParcelableExtra("bundle");




    }


    private void fetchLastLocation() {

        if (ActivityCompat.checkSelfPermission(this, Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED && ActivityCompat.checkSelfPermission(this, Manifest.permission.ACCESS_COARSE_LOCATION) != PackageManager.PERMISSION_GRANTED) {
            // TODO: Consider calling
            //    ActivityCompat#requestPermissions
            // here to request the missing permissions, and then overriding
            //   public void onRequestPermissionsResult(int requestCode, String[] permissions,
            //                                          int[] grantResults)
            // to handle the case where the user grants the permission. See the documentation
            // for ActivityCompat#requestPermissions for more details.
            return;
        }
        Task<Location> task = fusedLocationProviderClient.getLastLocation();
        task.addOnSuccessListener(new OnSuccessListener<Location>() {
            @Override
            public void onSuccess(Location location) {
                if (location != null) {
                    SupportMapFragment supportMapFragment = (SupportMapFragment) getSupportFragmentManager().findFragmentById(R.id.map);
                    supportMapFragment.getMapAsync(StartTripDriverActivity.this);
                } else {
                    Toast.makeText(StartTripDriverActivity.this, "No Location recorded", Toast.LENGTH_SHORT).show();
                }
            }
        });
    }

    @Override
    public void onMapReady(final GoogleMap googleMap) {

        tripService.getTripById(tripId.toString()).enqueue(new Callback<SerializedTripPostResponse>() {
            @Override
            public void onResponse(Call<SerializedTripPostResponse> call, Response<SerializedTripPostResponse> response) {

                SerializedTripPostResponse trip = response.body();

                Destination source = trip.getSource();
                originLocation = new LatLng(Double.valueOf(trip.getSource().getLat()), Double.valueOf(trip.getSource().getLong() ));
                destinynLocation = new LatLng(Double.valueOf(trip.getDestination().getLat()), Double.valueOf(trip.getDestination().getLong() ));

                //MarkerOptions are used to create a new Marker.You can specify location, title etc with MarkerOptions
                MarkerOptions markerOptions = new MarkerOptions().position(originLocation).title("Origen");

                boolean success = googleMap.setMapStyle(
                        MapStyleOptions.loadRawResourceStyle(
                                getApplicationContext(), R.raw.style_json));

                //Adding the created the marker on the map
                //markerOptions.icon(BitmapDescriptorFactory.defaultMarker(BitmapDescriptorFactory.HUE_BLUE));
                markerOptions.icon(BitmapDescriptorFactory.fromBitmap(resizeMapIcons(R.drawable.icon_marker, CUSTOM_MARKER_WIDTH, CUSTOM_MARKER_HEIGHT)));
                googleMap.addMarker(markerOptions);

                MarkerOptions destinyMarkerOptions = new MarkerOptions().position(destinynLocation).title("Destino");

                //Adding the created the marker on the map
                destinyMarkerOptions.icon(BitmapDescriptorFactory.fromBitmap(resizeMapIcons(R.drawable.icon_marker, CUSTOM_MARKER_WIDTH, CUSTOM_MARKER_HEIGHT)));
                googleMap.addMarker(destinyMarkerOptions);


                LatLngBounds.Builder bc = new LatLngBounds.Builder();


                List<LatLng> path = getPath(originLocation, destinynLocation);
                path.stream().forEach(pathPoint -> {
                    bc.include(pathPoint);
                });
                bc.include(destinynLocation).include(originLocation);
                addPoly(path, mMap);




                Handler h = new Handler();
                int delay = 4 * 100;
                int i = 0;
                MarkerOptions driverMarker = new MarkerOptions().position(path.get(i)).title("Chofer");
                driverMarker.icon(BitmapDescriptorFactory.fromBitmap(resizeMapIcons(R.drawable.icon_dog_car, CUSTOM_MARKER_WIDTH, CUSTOM_MARKER_HEIGHT)));

                Marker driver = googleMap.addMarker(driverMarker);

                googleMap.moveCamera(CameraUpdateFactory.newLatLngBounds(bc.build(), 40));
//                googleMap.animateCamera(CameraUpdateFactory.newLatLngBounds(bc.build(), 40));

            }

            @Override
            public void onFailure(Call<SerializedTripPostResponse> call, Throwable t) {

            }
        });

        mMap = googleMap;





    }

    public void addPoly(List<LatLng> path, GoogleMap mMap) {
        if (path.size() > 0) {
            PolylineOptions opts = new PolylineOptions().addAll(path).color(Color.parseColor("#6892E9")).width(10).jointType(JointType.ROUND);
            mMap.addPolyline(opts);
        }
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, String[] permissions, int[] grantResult) {
        switch (requestCode) {
            case LOCATION_REQUEST_CODE:
                if (grantResult.length > 0 && grantResult[0] == PackageManager.PERMISSION_GRANTED) {
                    fetchLastLocation();
                } else {
                    Toast.makeText(StartTripDriverActivity.this, "Location permission missing", Toast.LENGTH_SHORT).show();
                }
                break;
        }
    }

    public Bitmap resizeMapIcons(Integer iconId, int width, int height) {

        Bitmap imageBitmap = BitmapFactory.decodeResource(getResources(), iconId);
        Bitmap resizedBitmap = Bitmap.createScaledBitmap(imageBitmap, width, height, false);
        return resizedBitmap;
    }

    private void setOriginMarker(final GoogleMap googleMap) {
        MarkerOptions markerOptions = new MarkerOptions().position(originLocation).title("Origen");
        markerOptions.icon(BitmapDescriptorFactory.fromBitmap(resizeMapIcons(R.drawable.icon_marker, CUSTOM_MARKER_WIDTH, CUSTOM_MARKER_HEIGHT)));

        //Adding the created the marker on the map
        googleMap.addMarker(markerOptions);
    }

    private void setDestinyMarker(final GoogleMap googleMap) {
        MarkerOptions markerOptions = new MarkerOptions().position(destinynLocation).title("Destino");
        markerOptions.icon(BitmapDescriptorFactory.fromBitmap(resizeMapIcons(R.drawable.icon_marker, CUSTOM_MARKER_WIDTH, CUSTOM_MARKER_HEIGHT)));

        //Adding the created the marker on the map
        googleMap.addMarker(markerOptions);
    }

    private String getAddress(LatLng latLng) {
        // 1
        Geocoder geocoder = new Geocoder(this, Locale.getDefault());
        List<Address> addresses;

        Address address = null;
        try {
            // 2
            double longitude = latLng.longitude;
            double latitude = latLng.latitude;
            addresses = geocoder.getFromLocation(latitude, longitude, 1);
            // 3
            if (null != addresses && !addresses.isEmpty()) {
                address = addresses.get(0);
            }
        } catch (IOException e) {
            Log.e("MapsActivity", e.getMessage());

        }

        return address != null ? address.getAddressLine(0) : "";
    }


    public LatLng getLocation(String search) {
        // 1
        Geocoder geocoder = new Geocoder(this, Locale.getDefault());
        List<Address> addresses;

        Address address = null;
        try {
            addresses = geocoder.getFromLocationName(search, 1, -34.6879526, -58.5990012,
                    -34.528286, -58.346597);
            // 2
            // 3
            if (null != addresses && !addresses.isEmpty()) {
                address = addresses.get(0);
            }
        } catch (IOException e) {
            Log.e("MapsActivity", e.getMessage());

        }

        return address != null ? new LatLng(address.getLatitude(), address.getLongitude()) : null;
    }

    private String getStringFromLatLng(LatLng loc) {
        return String.valueOf(loc.latitude) + "," + String.valueOf(loc.longitude);
    }

    private List<LatLng> getPath(LatLng origin, LatLng destiny) {

        List<LatLng> path = new ArrayList();


        //Execute Directions API request
        GeoApiContext context = new GeoApiContext.Builder()
                .apiKey(getString(R.string.google_api_key))
                .build();
        DirectionsApiRequest req = DirectionsApi.getDirections(context, getStringFromLatLng(origin), getStringFromLatLng(destiny));
        try {
            DirectionsResult res = req.await();

            //Loop through legs and steps to get encoded polylines of each step
            if (res.routes != null && res.routes.length > 0) {
                DirectionsRoute route = res.routes[0];

                if (route.legs != null) {
                    for (int i = 0; i < route.legs.length; i++) {
                        DirectionsLeg leg = route.legs[i];
                        if (leg.steps != null) {
                            for (int j = 0; j < leg.steps.length; j++) {
                                DirectionsStep step = leg.steps[j];
                                if (step.steps != null && step.steps.length > 0) {
                                    for (int k = 0; k < step.steps.length; k++) {
                                        DirectionsStep step1 = step.steps[k];
                                        EncodedPolyline points1 = step1.polyline;
                                        if (points1 != null) {
                                            //Decode polyline and add points to list of route coordinates
                                            List<com.google.maps.model.LatLng> coords1 = points1.decodePath();
                                            for (com.google.maps.model.LatLng coord1 : coords1) {
                                                path.add(new LatLng(coord1.lat, coord1.lng));
                                            }
                                        }
                                    }
                                } else {
                                    EncodedPolyline points = step.polyline;
                                    if (points != null) {
                                        //Decode polyline and add points to list of route coordinates
                                        List<com.google.maps.model.LatLng> coords = points.decodePath();
                                        for (com.google.maps.model.LatLng coord : coords) {
                                            path.add(new LatLng(coord.lat, coord.lng));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception ex) {

        }
        return path;
    }
}