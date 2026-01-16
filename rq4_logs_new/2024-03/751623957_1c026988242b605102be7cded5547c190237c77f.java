package com.example.its_a_feature_not_a_bug;

import android.content.Intent;
import android.os.Bundle;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.appcompat.app.AppCompatActivity;

import java.text.SimpleDateFormat;


public class EventDetailsActivity extends AppCompatActivity {
    private Event event;
    private TextView name;
    private TextView date;
    private TextView location;
    private TextView description;
    private ImageView qrCode;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.event_details);

        name = findViewById(R.id.eventTitle);
        date = findViewById(R.id.eventDate);
        description = findViewById(R.id.eventDescription);

        Intent intent = getIntent();
        event = (Event) intent.getSerializableExtra("event");

        displayInfo();
    }

    public void displayInfo() {
        name.setText(event.getTitle());
        // convert date to string
        date.setText(event.getDate().toString());

        description.setText(event.getDescription());
    }
}