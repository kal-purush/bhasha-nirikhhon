package com.example.cmput301f20t18;

import android.location.Address;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.firebase.ui.firestore.FirestoreRecyclerAdapter;
import com.firebase.ui.firestore.FirestoreRecyclerOptions;

public class FirestoreLocationAdapter extends FirestoreRecyclerAdapter<Address, FirestoreLocationAdapter.locationViewHolder> {

    private int bookID;


    /**
     * Create a new RecyclerView adapter that listens to a Firestore Query.  See {@link
     * FirestoreRecyclerOptions} for configuration options.
     *
     * @param options
     */
    public FirestoreLocationAdapter(@NonNull FirestoreRecyclerOptions<Address> options, int bookID) {
        super(options);
        this.bookID = bookID;
    }

    @Override
    protected void onBindViewHolder(@NonNull FirestoreLocationAdapter.locationViewHolder holder, int position, @NonNull Address location) {

            holder.pickup_location.setText(SelectLocationActivity.getAddressString(location));
            holder.selectLocation.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    User current = new User();
                    current.ownerSetPickupLocation(location, bookID);
                }
            });

            holder.deleteLocation.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    User current = new User();
                    current.ownerDeleteLocation(SelectLocationActivity.getAddressString(location));
                }
            });
        }


    @NonNull
    @Override
    public FirestoreLocationAdapter.locationViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        LayoutInflater inflater = LayoutInflater.from(parent.getContext());
        return new locationViewHolder(inflater.inflate(R.layout.card_location, null));
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