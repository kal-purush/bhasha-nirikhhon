package com.example.cmput301f20t18;

import android.os.Build;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.annotation.RequiresApi;
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
        holder.borrower_name.setText(transaction.getBorrower_username());

        // hits the accept button
        holder.accept_button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                User current = new User();
                current.ownerAcceptRequest(transaction.getID());
            }
        });

        holder.delete_button.setOnClickListener(new View.OnClickListener() {
            @RequiresApi(api = Build.VERSION_CODES.N)
            @Override
            public void onClick(View v) {
                User current = new User();
                current.ownerDenyRequest(transaction.getID());
            }
        });
    }

    @NonNull
    @Override
    public requestViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        LayoutInflater inflater = LayoutInflater.from(parent.getContext());
        return new FirestoreRequestAdapter.requestViewHolder(inflater.inflate(R.layout.card_user_request, null));
    }




    public class requestViewHolder extends RecyclerView.ViewHolder {

        Button delete_button;
        Button accept_button;
        TextView borrower_name;


        public requestViewHolder(@NonNull View itemView) {
            super(itemView);
            delete_button = itemView.findViewById(R.id.button_delete_request);
            accept_button = itemView.findViewById(R.id.button_accept_request);
            borrower_name = itemView.findViewById(R.id.text_username);

        }





    }
}