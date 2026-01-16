package com.example.travelerreservation;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.view.ViewGroup;
import android.widget.AdapterView;
import android.widget.BaseAdapter;
import android.widget.ListView;

import java.util.List;

public class ActivityReservationHistory extends AppCompatActivity {

    private ListView reservationListView;
    private List<Reservation> reservationList;
    private ReservationAdapter reservationAdapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_reservation_history);

        getSupportActionBar().setTitle("Reservation History");
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);

        reservationListView = findViewById(R.id.reservationListView);
        reservationList = getReservationData(); // Replace with your data source

        // Create a custom adapter to display reservations
        reservationAdapter = new ReservationAdapter();
        reservationListView.setAdapter(reservationAdapter);

        // Handle item clicks to view reservation details
        reservationListView.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> parent, View view, int position, long id) {
                Reservation selectedReservation = reservationList.get(position);
                // Start a new activity to display details of the selected reservation
                // You can pass the reservation data to the details activity using an Intent
                Intent intent = new Intent(ActivityReservationHistory.this, ActivityReservationSummary.class);
                intent.putExtra("reservation", (CharSequence) selectedReservation);
                startActivity(intent);
            }
        });
    }

    // Custom adapter to populate the ListView with reservations
    private class ReservationAdapter extends BaseAdapter {

        @Override
        public int getCount() {
            return reservationList.size();
        }

        @Override
        public Object getItem(int position) {
            return reservationList.get(position);
        }

        @Override
        public long getItemId(int position) {
            return position;
        }

        @Override
        public View getView(int position, View convertView, ViewGroup parent) {
            // Implement the getView method to display each reservation item in the list
            // Similar to the "View Reservations" activity's ReservationAdapter
            // You can reuse the layout and logic from that adapter here
            // Display reservation details and buttons as needed
            // Handle "Edit" or "Remove" buttons for each reservation if required
            return convertView;
        }
    }

    // Replace this with your data retrieval logic
    private List<Reservation> getReservationData() {
        // Retrieve reservation data from your data source (e.g., database or API)
        // Return a list of Reservation objects
        // Each Reservation object should contain details of a booking
        return null; // Replace with your actual data
    }
}


