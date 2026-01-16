package com.example.angelia.term4androidappproject;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.view.View;
import android.widget.Button;

import com.example.angelia.term4androidappproject.Adapters.CalculatedItineraryAdapter;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;

import java.util.ArrayList;
import java.util.List;

public class ViewSingleItineraryActivity extends AppCompatActivity {

    RecyclerView recyclerView;
    Button buttonDelete;

    public static String ITEM_LOCATION_KEY = "item_locations";
    public static String ITEM_METHODS_KEY = "travel_methods";
    public static String ITEM_KEY_KEY = "so many keys";

    String[] placesToGo;
    String[] methodsOfTravel;
    String item_key;

    CalculatedItineraryAdapter calculatedItineraryAdapter;

    private FirebaseDatabase firebaseDatabase;
    private DatabaseReference itineraryDatabaseReference;

    private FirebaseAuth firebaseAuth;

    private String UID;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_view_single_itinerary);

        recyclerView = findViewById(R.id.viewSingleRecycler);
        buttonDelete = findViewById(R.id.buttonDeleteItinerary);

        // Getting all the extras from intent
        Intent intent = getIntent();
        placesToGo = intent.getStringExtra(ITEM_LOCATION_KEY).split(",");
        methodsOfTravel = intent.getStringExtra(ITEM_METHODS_KEY).split(",");
        item_key = intent.getStringExtra(ITEM_KEY_KEY);

        // Setting recycler view
        calculatedItineraryAdapter = new CalculatedItineraryAdapter(placesToGo, methodsOfTravel);
        RecyclerView.LayoutManager layoutManager = new LinearLayoutManager(this);
        recyclerView.setAdapter(calculatedItineraryAdapter);
        recyclerView.setLayoutManager(layoutManager);

        // Setup firebase stuff
        firebaseDatabase = FirebaseDatabase.getInstance();
        firebaseAuth = FirebaseAuth.getInstance();

        UID = firebaseAuth.getCurrentUser().getUid();
        itineraryDatabaseReference = firebaseDatabase.getReference().child(UID);

        // Set on click listener
        buttonDelete.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                itineraryDatabaseReference.child(item_key).removeValue();
                finish();
                Intent i = new Intent(v.getContext(),ViewItinerariesActivity.class);
                i.setFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP);
                startActivity(i);
            }
        });
    }
}