package com.example.its_a_feature_not_a_bug;

import android.view.View;

import androidx.appcompat.app.AppCompatActivity;


public class AddEvent {
    public void addEvent() {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_add_event);
        name = findViewById(R.id.eventName);
        date = findViewById(R.id.eventDate);
        location = findViewById(R.id.eventLocation);
        description = findViewById(R.id.eventDescription);
        time = findViewById(R.id.eventTime);
        createButton = findViewById(R.id.createButton);
        date.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                showDatePickerDialog();
            }
        });
        time.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                showTimePickerDialog();
            }
        });
        createButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                createEvent();
            }
        });
        public void onCreate(Bundle savedInstanceState) {
            View view = inflater.inflate(R.layout.fragment_add_event, container, false);



        }
    }
}