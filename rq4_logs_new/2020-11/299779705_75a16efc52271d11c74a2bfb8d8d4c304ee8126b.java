package com.example.cmput301f20t18;

import android.content.Intent;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.firebase.ui.firestore.FirestoreRecyclerAdapter;
import com.firebase.ui.firestore.FirestoreRecyclerOptions;

public class FirestoreBookAdapter extends FirestoreRecyclerAdapter<Book, FirestoreBookAdapter.bookViewHolder> {

    /**
     * Create a new RecyclerView adapter that listens to a Firestore Query.  See
     * {@link FirestoreRecyclerOptions} for configuration options.
     *
     * @param options
     */
    public FirestoreBookAdapter(FirestoreRecyclerOptions options) {
        super(options);
    }

    @Override
    protected void onBindViewHolder(bookViewHolder holder, int i, Book book) {

        //holder.imageView.setImageResource(R.drawable.default_cover);
        holder.textViewTitle.setText(book.getTitle());
        holder.textViewAuthor.setText(book.getAuthor());
        holder.textViewYear.setText(String.valueOf(book.getYear()));
        holder.textViewISBN.setText(String.valueOf(book.getISBN()));

        int status = book.getStatus();
        switch (status) {
            case Book.STATUS_AVAILABLE:
                //todo
                break;
            case Book.STATUS_REQUESTED:

                // User clicks the "View requests" button
                holder.buttonViewRequests.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        Intent intent = new Intent(v.getContext(), ViewRequestsActivity.class);
                        v.getContext().startActivity(intent);
                    }
                });
                break;
            case Book.STATUS_ACCEPTED:

                // User clicks the "Confirm pick up" button
                holder.buttonConfirmPickUp.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        Intent intent = new Intent(v.getContext(), Scanner.class);
                        v.getContext().startActivity(intent);
                        //TODO: upon successful scan, book status should be changed to "borrowed"
                        // and should be updated in firestore
                    }
                });

                // User clicks on profile photo
                holder.buttonUser.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        // TODO: open the users profile in a new activity
                    }
                });

                // User clicks on map button
                holder.buttonMap.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        Intent intent = new Intent(v.getContext(), ChooseLocationActivity.class);
                        v.getContext().startActivity(intent);
                    }
                });
                break;
            case Book.STATUS_BORROWED:

                // User clicks the "Confirm return" button
                holder.buttonConfirmReturn.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        Intent intent = new Intent(v.getContext(), Scanner.class);
                        v.getContext().startActivity(intent);
                        //TODO: upon successful scan, book status should be changed to "available"
                        // and should be updated in firestore
                    }
                });

                // User clicks on profile photo
                holder.buttonUser.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        // TODO: open the users profile in a new activity
                    }
                });

                // User clicks on map button
                holder.buttonMap.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        Intent intent = new Intent(v.getContext(), ChooseLocationActivity.class);
                        v.getContext().startActivity(intent);
                    }
                });
                break;
        }


    }


    @NonNull
    @Override
    public bookViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {

        LayoutInflater inflater = LayoutInflater.from(parent.getContext());

        switch (viewType) {
            case Book.STATUS_AVAILABLE:
                return new FirestoreBookAdapter.bookViewHolder(inflater.inflate(R.layout.card_mybooks_no_requests, null));
            case Book.STATUS_REQUESTED:
                return new FirestoreBookAdapter.bookViewHolder(inflater.inflate(R.layout.card_mybooks_requested, null));
            case Book.STATUS_ACCEPTED:
                return new FirestoreBookAdapter.bookViewHolder(inflater.inflate(R.layout.card_mybooks_pending, null));
            case Book.STATUS_BORROWED:
                return new FirestoreBookAdapter.bookViewHolder(inflater.inflate(R.layout.card_mybooks_lending, null));
            default: Log.d("firestoreadapter", "Error: Book status not found");
        }

        View view = inflater.inflate(R.layout.card_mybooks_no_requests, null);
        return new FirestoreBookAdapter.bookViewHolder(view);
    }


    public class bookViewHolder extends RecyclerView.ViewHolder {
        ImageView imageView;
        TextView textViewTitle;
        TextView textViewAuthor;
        TextView textViewYear;
        TextView textViewISBN;
        TextView textViewStatus;

        Button buttonViewRequests;
        Button buttonConfirmPickUp;
        Button buttonMap;
        Button buttonUser;
        Button buttonConfirmReturn;

        public bookViewHolder(@NonNull View itemView) {
            super(itemView);
            imageView = itemView.findViewById(R.id.image_view);

            textViewTitle = itemView.findViewById(R.id.text_book_title);
            textViewAuthor = itemView.findViewById(R.id.text_book_author);
            textViewYear = itemView.findViewById(R.id.text_book_year);
            textViewISBN = itemView.findViewById(R.id.text_book_isbn);

            buttonViewRequests = itemView.findViewById(R.id.button_view_requests);
            buttonConfirmPickUp = itemView.findViewById(R.id.button_confirm_pick_up);
            buttonMap = itemView.findViewById(R.id.button_mybooks_map);
            buttonUser = itemView.findViewById(R.id.button_mybooks_user);
            buttonConfirmReturn = itemView.findViewById(R.id.button_confirm_return);
        }

    }
}
