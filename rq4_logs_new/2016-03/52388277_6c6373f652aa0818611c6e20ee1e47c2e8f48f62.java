package msj.hellocampus_android;

import android.support.v4.app.FragmentActivity;
import android.os.Bundle;

import com.firebase.client.DataSnapshot;
import com.firebase.client.Firebase;
import com.firebase.client.FirebaseError;
import com.firebase.client.ValueEventListener;
import com.google.android.gms.maps.CameraUpdateFactory;
import com.google.android.gms.maps.GoogleMap;
import com.google.android.gms.maps.OnMapReadyCallback;
import com.google.android.gms.maps.SupportMapFragment;
import com.google.android.gms.maps.model.LatLng;
import com.google.android.gms.maps.model.MarkerOptions;

public class MapsActivity extends FragmentActivity implements OnMapReadyCallback {

    private GoogleMap mMap;
    private double lng;
    private double lat;
    Firebase mNodesRef;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_maps);
        Bundle b = getIntent().getExtras();
        lat = b.getDouble("lat");
        lng = b.getDouble("lng");
        mNodesRef = new Firebase(getResources().getString(R.string.firebase_url)).child("nodes");

        // Obtain the SupportMapFragment and get notified when the map is ready to be used.
        SupportMapFragment mapFragment = (SupportMapFragment) getSupportFragmentManager()
                .findFragmentById(R.id.map);
        mapFragment.getMapAsync(this);
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

        mNodesRef.addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot snapshot) {
                System.out.println("There are " + snapshot.getChildrenCount() + " nodes");
                for (DataSnapshot nodeSnapshot : snapshot.getChildren()) {
                    Node node = nodeSnapshot.getValue(Node.class);
                    double a = node.getLatitude();
                    System.out.println("lat:" + a);
                    double b = node.getLongitude();
                    System.out.println("lng:" + b);
                    String type = node.getType();
                    if (a != 0 && b != 0) {
                        LatLng bob = new LatLng(a, b);
                        mMap.addMarker(new MarkerOptions().position(bob).title(type));

                    }
                    }
                }
            @Override
            public void onCancelled(FirebaseError firebaseError) {
                System.out.println("The read failed: " + firebaseError.getMessage());
            }
        });
        // Add a marker in Sydney and move the camera
        LatLng ucsb = new LatLng(34.4137563, -119.8412435);
        mMap.addMarker(new MarkerOptions().position(ucsb).title("Your Location"));
        mMap.moveCamera(CameraUpdateFactory.newLatLngZoom(ucsb, 16));
       // mMap.moveCamera(CameraUpdateFactory.newLatLng(ucsb));
    }
}