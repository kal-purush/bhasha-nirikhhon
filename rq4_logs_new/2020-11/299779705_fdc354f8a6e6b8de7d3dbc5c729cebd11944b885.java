package com.example.cmput301f20t18;

import android.app.Activity;
import android.content.Context;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.firebase.ui.firestore.FirestoreRecyclerAdapter;
import com.firebase.ui.firestore.FirestoreRecyclerOptions;

public class FirestoreInitalizeLocation extends FirestoreRecyclerAdapter<UserLocation, FirestoreInitalizeLocation.locationViewHolder> {

    private int bookID;
    private int t_id;
    static final String TAG = "FLA_DEBUG";
    Context context;


    public FirestoreInitalizeLocation(@NonNull FirestoreRecyclerOptions<UserLocation> options, int bookID, Context context, int t_id) {
        super(options);
        this.bookID = bookID;
        this.context = context;
        this.t_id = t_id;
    }

    @Override
    protected void onBindViewHolder(@NonNull FirestoreInitalizeLocation.locationViewHolder holder, int position, @NonNull UserLocation location) {
        Log.d(TAG, "onBindViewHolder: Title: " + location.getTitle());
        holder.pickup_location.setText(location.getTitle());
        holder.selectLocation.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                User current = new User();
                current.ownerAcceptRequest(t_id, location, bookID);
                ((Activity)context).finish();
            }
        });


        // owner deletes a location from the their pickup_locations
        holder.deleteLocation.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                User current = new User();
                current.ownerDeleteLocation(location);
            }
        });
    }

    @NonNull
    @Override
    public FirestoreInitalizeLocation.locationViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        LayoutInflater inflater = LayoutInflater.from(parent.getContext());
        return new FirestoreInitalizeLocation.locationViewHolder(inflater.inflate(R.layout.card_location, null));
    }

    public class locationViewHolder extends RecyclerView.ViewHolder {
        TextView pickup_location;
        Button selectLocation;
        Button deleteLocation;


        public locationViewHolder(@NonNull View itemView) {
            super(itemView);
            pickup_location = itemView.findViewById(R.id.text_address);
            selectLocation = itemView.findViewById(R.id.button_select_location);
            deleteLocation = itemView.findViewById(R.id.button_delete_location);
        }
    }
}