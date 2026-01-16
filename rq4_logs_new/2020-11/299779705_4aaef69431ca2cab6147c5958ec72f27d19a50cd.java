package com.example.cmput301f20t18;

import android.app.Activity;
import android.content.DialogInterface;
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
import androidx.appcompat.app.AlertDialog;
import androidx.core.content.ContextCompat;
import androidx.recyclerview.widget.RecyclerView;

import com.firebase.ui.firestore.FirestoreRecyclerAdapter;
import com.firebase.ui.firestore.FirestoreRecyclerOptions;

public class FirestoreBorrowedAdapter extends FirestoreRecyclerAdapter<Book, FirestoreBorrowedAdapter.bookViewHolderBorrowed> {
    final static String TAG = "FBA_DEBUG";
    final static int FRAG_PICKUP = 2;
    final static int FRAG_RETURN = 1;


    /**
     * Create a new RecyclerView adapter that listens to a Firestore Query.  See
     * {@link FirestoreRecyclerOptions} for configuration options.
     *
     * @param options
     */
    public FirestoreBorrowedAdapter(FirestoreRecyclerOptions options) {
        super(options);
    }




    @Override
    public int getItemViewType(int position) {
        switch (getItem(position).getStatus()) {
            case Book.STATUS_AVAILABLE:
                return Book.STATUS_AVAILABLE;

            case Book.STATUS_REQUESTED:
                return Book.STATUS_REQUESTED;

            case Book.STATUS_BORROWED:
                return Book.STATUS_BORROWED;

            case Book.STATUS_ACCEPTED:
                return Book.STATUS_ACCEPTED;
        }
        return 0;
    }


    @Override
    protected void onBindViewHolder(FirestoreBorrowedAdapter.bookViewHolderBorrowed holder, int i, Book book) {
        /* TODO: Retrieve cover photo from database and assign it to imageView. */
        //holder.imageView.setImageResource(R.drawable.default_cover);

        holder.textViewTitle.setText(book.getTitle());
        holder.textViewAuthor.setText(book.getAuthor());
        holder.textViewYear.setText(String.valueOf(book.getYear()));
        holder.textViewISBN.setText(String.valueOf(book.getISBN()));

        /* TODO: Implement cancel pick up UI and functionality (for "accepted" books) */

        try {
            /* These two TextViews will be null if the book status is "Available" */
            /* TODO: Retrieve username of borrower and assign it to textViewUsername. */
            holder.textViewUsername.setText("Username");
            holder.textViewUserDescription.setText(R.string.owner);
        } catch (Exception e) {
            Log.e(TAG, e.toString());
        }

        try {
            /* User clicks on profile photo. This button is in all three tabs. */
            holder.buttonUser.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    Intent intent = new Intent(v.getContext(), CheckProfileActivity.class);
                    v.getContext().startActivity(intent);
                }
            });
        } catch (Exception e) {}


        try {
            /* User clicks on map button. This button is in all three tabs. */
            holder.buttonMap.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    /* Are we allowing the borrower to see the location before being accepted? */
                    /* TODO: make activity that displays pick up location to borrower */
                }
            });
        } catch (Exception e) {}

        /* holder will be updated differently depending on Book status. */
        int status = book.getStatus();
        switch (status) {
            case Book.STATUS_AVAILABLE:
                /* Books in Borrowed section should never have status "available" */
                Log.e(TAG, "onBindViewHolder: \"Available\" status not allowed in borrowed section.");
                break;

            case Book.STATUS_REQUESTED:
                /* User clicks the "Cancel request" button */
                holder.buttonCancelRequest.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        AlertDialog dialog = (new AlertDialog.Builder(v.getContext())
                                .setTitle("Cancel request")
                                .setMessage("Are you sure you want to cancel your request?")
                                .setPositiveButton("Yes", new DialogInterface.OnClickListener() {
                                    @Override
                                    public void onClick(DialogInterface dialog, int which) {
                                        /* TODO: Actually cancel request */
                                    }
                                })
                                .setNegativeButton("No", null)).show();

                        Button buttonPositive = dialog.getButton(DialogInterface.BUTTON_POSITIVE);
                        buttonPositive.setTextColor(ContextCompat.getColor(v.getContext(), R.color.colorPrimaryDark));
                        Button buttonNegative = dialog.getButton(DialogInterface.BUTTON_NEGATIVE);
                        buttonNegative.setTextColor(ContextCompat.getColor(v.getContext(), R.color.colorPrimaryDark));
                    }
                });
                break;

            case Book.STATUS_ACCEPTED:
                /* User clicks the "Confirm pick up" button */
                holder.buttonConfirmPickUp.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        // TODO: implement ISBN check
                        Intent intent = new Intent(v.getContext(), Scanner.class);
                        intent.putExtra("bookID", book.getId());
                        intent.putExtra("type", 1);
                        Activity main = (Activity) v.getContext();
                        main.startActivityForResult(intent, FRAG_PICKUP);
                    }
                });

                /* User clicks the 3 dots "more" button */
                holder.buttonMore.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        /* TODO: create custom context menu for Cancel request */
                        new android.app.AlertDialog.Builder(v.getContext())
                                .setTitle("TODO: Cancel request").show();
                    }
                });
                break;

            case Book.STATUS_BORROWED:

                /* User clicks the "Confirm return" button */
                holder.buttonConfirmReturn.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        Intent intent = new Intent(v.getContext(), Scanner.class);
                        intent.putExtra("bookID", book.getId());
                        intent.putExtra("type", 1);
                        Activity main = (Activity) v.getContext();
                        main.startActivityForResult(intent, FRAG_RETURN);



                    }
                });
                break;
        }
    }




    @NonNull
    @Override
    public bookViewHolderBorrowed onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        LayoutInflater inflater = LayoutInflater.from(parent.getContext());
        Log.d(TAG, "viewType: : " + Integer.toString(viewType));

        switch (viewType) {
            case Book.STATUS_AVAILABLE:
                Log.d(TAG, "onCreateViewHolder: Got to available!!");
                return new FirestoreBorrowedAdapter.bookViewHolderBorrowed(inflater.inflate(R.layout.card_no_requests, null));
            case Book.STATUS_REQUESTED:
                Log.d(TAG, "onCreateViewHolder: Got to requested!");
                return new FirestoreBorrowedAdapter.bookViewHolderBorrowed(inflater.inflate(R.layout.card_borrowed_requested, null));
            case Book.STATUS_ACCEPTED:
                return new FirestoreBorrowedAdapter.bookViewHolderBorrowed(inflater.inflate(R.layout.card_pending, null));
            case Book.STATUS_BORROWED:
                return new FirestoreBorrowedAdapter.bookViewHolderBorrowed(inflater.inflate(R.layout.card_borrowed_lending, null));
            default: Log.d(TAG, "Error: Book status not found");
        }

        return null;
    }


    public class bookViewHolderBorrowed extends RecyclerView.ViewHolder {

        ImageView imageView;
        TextView textViewTitle;
        TextView textViewAuthor;
        TextView textViewYear;
        TextView textViewISBN;

        TextView textViewUsername;
        TextView textViewUserDescription;

        Button buttonCancelRequest;
        Button buttonConfirmPickUp;
        Button buttonMap;
        Button buttonUser;
        Button buttonConfirmReturn;
        Button buttonMore;


        public bookViewHolderBorrowed(@NonNull View itemView) {
            super(itemView);

            imageView = itemView.findViewById(R.id.image_view);

            textViewTitle = itemView.findViewById(R.id.text_book_title);
            textViewAuthor = itemView.findViewById(R.id.text_book_author);
            textViewYear = itemView.findViewById(R.id.text_book_year);
            textViewISBN = itemView.findViewById(R.id.text_book_isbn);

            textViewUsername = itemView.findViewById(R.id.text_username);
            textViewUserDescription = itemView.findViewById(R.id.text_user_description);

            buttonCancelRequest = itemView.findViewById(R.id.button_cancel_request);
            buttonConfirmPickUp = itemView.findViewById(R.id.button_confirm_pick_up);
            buttonMap = itemView.findViewById(R.id.button_mybooks_map);
            buttonUser = itemView.findViewById(R.id.button_mybooks_user);
            buttonConfirmReturn = itemView.findViewById(R.id.button_confirm_return);
            buttonMore = itemView.findViewById(R.id.button_book_more);
        }

    }

}