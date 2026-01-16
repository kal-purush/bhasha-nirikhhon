package com.example.group2_project3;

import android.os.Bundle;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;

import com.example.group2_project3.Model.Vehicles;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;
import com.rey.material.widget.FloatingActionButton;
import com.squareup.picasso.Picasso;

public class DetailActivity extends AppCompatActivity {

    private Button addToCartBtn;
    private ImageView vehicleImage;
    private TextView vehicleName, vehiclePrice, vehicleDescription;
    private String vehicleID = "";

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_detail);

        addToCartBtn = findViewById(R.id.addto_cart_button);
        vehicleImage = findViewById(R.id.product_image);
        vehicleName = findViewById(R.id.product_name);
        vehiclePrice = findViewById(R.id.product_price);
        vehicleDescription = findViewById(R.id.product_description);

        vehicleID = getIntent().getStringExtra("vid");
        getVehicleDetails(vehicleID);

    }

    private void getVehicleDetails(String vehicleID) {
        DatabaseReference vehiclesRef = FirebaseDatabase.getInstance().getReference().child("Vehicles");

        vehiclesRef.child(vehicleID).addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(@NonNull DataSnapshot snapshot) {
                if (snapshot.exists()){
                    Vehicles vehicles = snapshot.getValue(Vehicles.class);
                    vehicleName.setText(vehicles.getvName());
                    vehiclePrice.setText(vehicles.getPrice());
                    vehicleDescription.setText(vehicles.getDescription());
                    Picasso.get().load(vehicles.getImage()).into(vehicleImage);

                }
            }

            @Override
            public void onCancelled(@NonNull DatabaseError error) {

            }
        });

    }
}