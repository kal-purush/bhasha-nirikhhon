package com.example.cmput301f20t18;

import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.firebase.ui.firestore.FirestoreRecyclerAdapter;
import com.firebase.ui.firestore.FirestoreRecyclerOptions;

public class FirestoreNotificationAdapter extends FirestoreRecyclerAdapter<String, FirestoreNotificationAdapter.NotifViewHolder> {


    /**
     * Create a new RecyclerView adapter that listens to a Firestore Query.  See {@link
     * FirestoreRecyclerOptions} for configuration options.
     *
     * @param options
     */
    public FirestoreNotificationAdapter(@NonNull FirestoreRecyclerOptions<String> options) {
        super(options);
    }

    @Override
    protected void onBindViewHolder(@NonNull NotifViewHolder holder, int position, @NonNull String model) {
        holder.notification_text.setText(model);
    }

    @NonNull
    @Override
    public NotifViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        return null;
    }


    public class NotifViewHolder extends RecyclerView.ViewHolder {

        TextView notification_text;

        public NotifViewHolder(@NonNull View itemView) {
            super(itemView);
        }
    }




}