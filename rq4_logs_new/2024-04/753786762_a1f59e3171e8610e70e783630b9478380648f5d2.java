package com.example.fruitseason;

import androidx.fragment.app.FragmentActivity;

import android.location.Address;
import android.location.Geocoder;
import android.os.Bundle;
import android.util.Log;
import android.widget.TextView;

import com.example.fruitseason.model.Model;
import com.google.android.gms.maps.CameraUpdateFactory;
import com.google.android.gms.maps.GoogleMap;
import com.google.android.gms.maps.OnMapReadyCallback;
import com.google.android.gms.maps.SupportMapFragment;
import com.google.android.gms.maps.model.BitmapDescriptorFactory;
import com.google.android.gms.maps.model.LatLng;
import com.google.android.gms.maps.model.MarkerOptions;
import com.example.fruitseason.databinding.ActivitySellerMapsBinding;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

public class SellerMapsActivity extends FragmentActivity implements OnMapReadyCallback {

    private GoogleMap mMap;
    private ActivitySellerMapsBinding binding;
    private TextView txtOpeningHours, txtAddress, txtName;
    private Model selectedSeller;
    private String fullAddress;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        binding = ActivitySellerMapsBinding.inflate(getLayoutInflater());
        setContentView(binding.getRoot());

        txtAddress = findViewById(R.id.txtAddressDetails);
        txtOpeningHours = findViewById(R.id.txtOpeningHours);
        txtName = findViewById(R.id.nameSellerDetails);

        selectedSeller = (Model) getIntent().getSerializableExtra("selectedSeller");

        if(selectedSeller != null){
            txtAddress.setText(selectedSeller.getFullAddress());
            txtName.setText(selectedSeller.getName());
            fullAddress = selectedSeller.getFullAddress();
        }

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

        Geocoder geocoder = new Geocoder(getApplicationContext(), Locale.getDefault() );

        try {

            String addressToSearch = fullAddress;
            List<Address> addressList = geocoder.getFromLocationName(addressToSearch,1);
            if( addressList != null && addressList.size() > 0 ){
                Address address = addressList.get(0);

                /*
                 * onLocationChanged:
                 * Address[
                 *   addressLines=[0:"Av. República do Líbano, 1291 - Parque Ibirapuera, São Paulo - SP, Brazil"],
                 *   feature=1291,
                 *   admin=São Paulo,
                 *   sub-admin=São Paulo,
                 *   locality=São Paulo,
                 *   thoroughfare=Avenida República do Líbano,
                 *   postalCode=null,
                 *   countryCode=BR,
                 *   countryName=Brazil,
                 *   hasLatitude=true,
                 *   latitude=-23.5926719,
                 *   hasLongitude=true,
                 *   longitude=-46.6647561,
                 *   phone=null,
                 *   url=null,
                 *   extras=null]
                 * */

                Double lat = address.getLatitude();
                Double lon = address.getLongitude();

                mMap.clear();
                LatLng sellerLocation = new LatLng(lat, lon);
                mMap.addMarker(new MarkerOptions().position(sellerLocation).title("Seller Location"));
                mMap.moveCamera(CameraUpdateFactory.newLatLngZoom(sellerLocation,15));
            }

        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
        

    }
}