package com.kanyi.bigtuna.activities;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.RecyclerView;

import android.content.Intent;
import android.location.Geocoder;
import android.os.Bundle;
import android.view.View;
import android.widget.ImageButton;
import android.widget.TextView;

import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;

import com.kanyi.bigtuna.R;
import com.kanyi.bigtuna.adapter.AdapterOrderedItem;
import com.kanyi.bigtuna.models.ModelOrderedItem;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class OrderDetailsUsersActivity extends AppCompatActivity {

    private String orderTo, orderId;

    private ImageButton backBtn;
    private TextView orderIdTv, dateTv, orderStatusTv, shopNameTv, totalItemsTv, amountTv, addressTv;
    private RecyclerView itemsRv;

    private FirebaseAuth firebaseAuth;

    private ArrayList<ModelOrderedItem> orderedItemArrayList;
    private AdapterOrderedItem adapterOrderedItem;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_order_details_users);

        backBtn = findViewById(R.id.backBtn);
        orderIdTv = findViewById(R.id.orderIdTv);
        dateTv = findViewById(R.id.dateTv);
        orderStatusTv = findViewById(R.id.orderStatusTv);
        shopNameTv = findViewById(R.id.shopNameTv);
        totalItemsTv = findViewById(R.id.totalItemsTv);
        amountTv = findViewById(R.id.amountTv);
        addressTv = findViewById(R.id.addressTv);
        itemsRv = findViewById(R.id.itemsRv);

        Intent intent = getIntent();
        orderTo = intent.getStringExtra("orderTo");
        orderId = intent.getStringExtra("orderId");

        firebaseAuth = FirebaseAuth.getInstance();
        loadShopInfo();
        loadOrderDetails();
        loadOrderedItems();

        backBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                onBackPressed();
            }
        });

    }

    private void loadOrderedItems() {
        orderedItemArrayList = new ArrayList<>();
        DatabaseReference ref = FirebaseDatabase.getInstance().getReference("Users");
        ref.child(orderTo).child("Orders").child(orderId).child("Items")
                .addValueEventListener(new ValueEventListener() {
                    @Override
                    public void onDataChange(@NonNull DataSnapshot snapshot) {

                        orderedItemArrayList.clear();
                        for (DataSnapshot ds: dataSnapshot.getChildren()){
                            ModelOrderedItem modelOrderedItem = ds.getValue(ModelOrderedItem.class);

                            orderedItemArrayList.add(modelOrderedItem);
                        }
                        adapterOrderedItem = new AdapterOrderedItem(OrderDetailsUsersActivity.this, orderedItemArrayList);
                        itemsRv.setAdapter(adapterOrderedItem);

                        totalItemsTv.setText(""+dataSnapshot.getChildrenCount());
                    }

                    @Override
                    public void onCancelled(@NonNull DatabaseError error) {

                    }
                });

    }

    private void loadOrderDetails() {

        DatabaseReference ref = FirebaseDatabase.getInstance().getReference("Users");
        ref.child(orderTo).child("Orders").child(orderId)
                .addValueEventListener(new ValueEventListener() {
                    @Override
                    public void onDataChange(@NonNull DataSnapshot snapshot) {

                        String orderBy =""+dataSnapshot.child("orderBy").getValue();
                        String orderCost =""+dataSnapshot.child("orderCost").getValue();
                        String orderId =""+dataSnapshot.child("orderId").getValue();
                        String orderStatus =""+dataSnapshot.child("orderStatus").getValue();
                        String orderTime =""+dataSnapshot.child("orderTime").getValue();
                        String orderTo =""+dataSnapshot.child("orderTo").getValue();
                        String deliveryFee =""+dataSnapshot.child("deliveryFee").getValue();
                        String latitude =""+dataSnapshot.child("latitude").getValue();
                        String longitude =""+dataSnapshot.child("longitude").getValue();

                    Calender calender = Calender.getInstance();
                    calender.setTimeInMillis(long.parseLong(orderTime));
                    String formatedDate = DateFormat.format("dd/MM/yyyy hh:mm a", calender).toString();

                    if(orderStatus.equals("In Progress")){
                        orderStatusTv.setTextColor(getResources().getColor(R.color.colorPrimary));

                    }else if(orderStatus.equals("Completed")){
                        orderStatusTv.setTextColor(getResources().getColor(R.color.colorGreen));

                    }else if(orderStatus.equals("Cancelled")){
                        orderStatusTv.setTextColor(getResources().getColor(R.color.colorRed));

                    }

                    orderIdTv.setText(orderId);
                    orderStatusTv.setText(orderStatus);
                    amountTv.setText("$"+orderCost+"[Including delivery fee $"+ deliveryFee+"]");
                    dateTv.setText(formatedDate);

                    findAddress(latitude,longitude);

                    }

                    @Override
                    public void onCancelled(@NonNull DatabaseError error) {

                    }
                });

    }

    private void loadShopInfo() {

        DatabaseReference ref = FirebaseDatabase.getInstance().getReference("Users");
        ref.child(orderTo)
                .addValueEventListener(new ValueEventListener() {
                    @Override
                    public void onDataChange(@NonNull DataSnapshot snapshot) {

                        String shopName = "" + dataSnapshot.child("shopName").getValue();
                        shopNameTv.setText(shopName);
                    }

                    @Override
                    public void onCancelled(@NonNull DatabaseError error) {

                    }
                });

    }
        private void findAddress(String latitude, String longitude) {

        double lat = Double.parseDouble(latitude);
        double lon = Double.parseDouble(longitude);

            Geocoder geocoder;
            List<address> addresses;
            geocoder = new Geocoder(this, Locale.getDefault());

            try {
                addresses = geocoder.getFromLocation(lat,lon, 1);

                String address = addresses.get(0).getAddresssLine(0);
                addressTv.setText(address);
            }
            catch (Exception e){

            }
    }
}