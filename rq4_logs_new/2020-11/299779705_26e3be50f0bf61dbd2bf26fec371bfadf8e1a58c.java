package com.example.cmput301f20t18;

import android.view.View;
import android.view.ViewGroup;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.firebase.ui.firestore.FirestoreRecyclerAdapter;
import com.firebase.ui.firestore.FirestoreRecyclerOptions;

public class FirestoreRequestAdapter extends FirestoreRecyclerAdapter<Transaction, FirestoreRequestAdapter.requestViewHolder> {

    final static String TAG = "FRA_DEBUG";

    /**
     * Create a new RecyclerView adapter that listens to a Firestore Query.  See {@link
     * FirestoreRecyclerOptions} for configuration options.
     *
     * @param options
     */
    public FirestoreRequestAdapter(@NonNull FirestoreRecyclerOptions<Transaction> options) {
        super(options);
    }





    @Override
    protected void onBindViewHolder(@NonNull FirestoreRequestAdapter.requestViewHolder holder, int position, @NonNull Transaction transaction) {

    }

    @NonNull
    @Override
    public requestViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        return null;
    }




    public class requestViewHolder extends RecyclerView.ViewHolder {

        public requestViewHolder(@NonNull View itemView) {
            super(itemView);
        }



    }
}