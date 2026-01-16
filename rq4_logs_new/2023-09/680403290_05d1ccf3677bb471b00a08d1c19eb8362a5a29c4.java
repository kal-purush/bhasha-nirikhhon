package com.example.virtualwaiter;

import android.util.Log;

import com.example.virtualwaiter.datatypes.Booking;
import com.google.firebase.firestore.DocumentChange;
import com.google.firebase.firestore.FirebaseFirestore;
import com.google.firebase.firestore.QueryDocumentSnapshot;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;

public class BookingManager {
    public ArrayList<Booking> bookings;
    public Integer tableID;
    public FirebaseFirestore db = FirebaseFirestore.getInstance();
    private BookingCallback callback;
    public interface BookingCallback {
        void onBookingReceived();
    }

    public BookingManager(Integer tableID){
        this.tableID = tableID;
        this.bookings = new ArrayList<>();
        db.collection("tableBookings").whereEqualTo("tableID", tableID).get().addOnCompleteListener(task -> {
            if (task.isSuccessful()) {
                for (QueryDocumentSnapshot document : task.getResult()) {;
                    Booking booking = new Booking(document.getString("name"), document.getLong("tableID").intValue(), document.getString("email"),document.getTimestamp("dateTime"));
                    Log.d("FirestoreData",  " booking " + document.getData());
                    bookings.add(booking);
                    long now=new Date().getTime();
                    bookings.removeIf(item -> item.dateTime.toDate().getTime() - now < 0);
                    bookings.sort(Comparator.comparing(o -> o.dateTime));
                    callback.onBookingReceived();
                }
            } else {
                Log.d("FirestoreData", "Error getting bookings: ", task.getException());
            }
        });
        db.collection("tableBookings").addSnapshotListener(((value, error) -> {
            if (error != null) {
                Log.w("FirestoreData", "tableBookings listen failed.", error);
                return;
            }
            for (DocumentChange change : value.getDocumentChanges()) {
                if(change.getDocument().getLong("tableID").intValue() != tableID){
                    continue;
                }
                switch (change.getType()) {
                    case ADDED:
                        Booking booking = new Booking(change.getDocument().getString("name"), change.getDocument().getLong("tableID").intValue(), change.getDocument().getString("email"),change.getDocument().getTimestamp("dateTime"));
                        Log.d("FirestoreData",  " booking " + change.getDocument().getData());
                        bookings.add(booking);
                        long now=new Date().getTime();
                        bookings.removeIf(item -> item.dateTime.toDate().getTime() - now < 0);
                        bookings.sort(Comparator.comparing(o -> o.dateTime));
                        Log.d("FirestoreData", "bookings size: "+bookings.size());
                        break;
                    case MODIFIED:
                        Log.d("FirestoreData", "Modified booking: ");
                        break;
                    case REMOVED:
                        Log.d("FirestoreData", "Removed booking: ");
                        break;
                }
            }
        }));
    }
    public Booking getNextBooking(){
        if(bookings.size() == 0){
            return null;
        }
        return bookings.get(0);
    }
    public void setBookingCallback(BookingCallback callback) {
        this.callback = callback;
    }
}