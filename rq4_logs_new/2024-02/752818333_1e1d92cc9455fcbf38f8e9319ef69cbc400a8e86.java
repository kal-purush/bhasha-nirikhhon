package com.android.quemeful_qr;

import android.os.Bundle;

import android.view.View;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.appcompat.app.AppCompatActivity;

import com.google.firebase.firestore.FirebaseFirestore;

public class EventDetailsActivity extends AppCompatActivity {


    private TextView textViewEventTitle, textViewEventDate, textViewEventTime, textViewEventLocation, textViewEventDescription;
    private FirebaseFirestore db;
    private ImageView imageViewBackArrow;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.eventdetailsactivity);

        db = FirebaseFirestore.getInstance();

        imageViewBackArrow = findViewById(R.id.backArrow);
        imageViewBackArrow.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // This will close the current activity and return to the previous one in the stack
                finish();
            }
        });

        textViewEventTitle = findViewById(R.id.textViewEventTitle);
        textViewEventDate = findViewById(R.id.textViewEventDate);
        textViewEventTime = findViewById(R.id.textViewEventTime);
        textViewEventLocation = findViewById(R.id.textViewEventLocation);
        textViewEventDescription = findViewById(R.id.textViewEventDescription);

        String eventId = getIntent().getStringExtra("event_id");
        if (eventId != null) {
            fetchEventDetails(eventId);
        } else {
            // Handle the error
        }
    }

    private void fetchEventDetails (String eventId) {
        db.collection("events").document(eventId).get().addOnSuccessListener(documentSnapshot -> {
            if (documentSnapshot.exists()) {
                EventHelper event = documentSnapshot.toObject(EventHelper.class);
                if (event != null) {
                    textViewEventTitle.setText(event.getTitle());
                    textViewEventDate.setText(event.getDate());
                    textViewEventTime.setText(event.getTime());
                    textViewEventLocation.setText(event.getLocation());
                    textViewEventDescription.setText(event.getDescription());
                }
            } else {
            }
        }).addOnFailureListener(e -> {
        });
    }
}