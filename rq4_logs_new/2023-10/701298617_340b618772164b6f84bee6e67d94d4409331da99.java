package com.example.travelerreservation;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class ActivityReservationSummary extends AppCompatActivity {
    private TextView customerNameSummaryTextView;
    private TextView trainNameSummaryTextView;
    private TextView dateAndTimeSummaryTextView;
    private Button confirmButton;
    private Button cancelButton;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_reservation_summary);

        customerNameSummaryTextView = findViewById(R.id.customerNameSummaryTextView);
        trainNameSummaryTextView = findViewById(R.id.trainNameSummaryTextView);
        dateAndTimeSummaryTextView = findViewById(R.id.dateAndTimeSummaryTextView);
        confirmButton = findViewById(R.id.confirmButton);
        cancelButton = findViewById(R.id.cancelButton);

        // Retrieve reservation data from the Intent
        Intent intent = getIntent();
        String customerName = intent.getStringExtra("customerName");
        String trainName = intent.getStringExtra("trainName");
        String dateAndTime = intent.getStringExtra("dateAndTime");

        // Display reservation details in TextViews
        customerNameSummaryTextView.setText(customerName);
        trainNameSummaryTextView.setText(trainName);
        dateAndTimeSummaryTextView.setText(dateAndTime);

        confirmButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // Handle confirmation (e.g., edit or delete reservation)
                // You can send the result back to the "View Reservations" activity if needed
                setResult(RESULT_OK);
                finish();
            }
        });

        cancelButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // Cancel and return to the "View Reservations" activity without making changes
                setResult(RESULT_CANCELED);
                finish();
            }
        });
    }
}
