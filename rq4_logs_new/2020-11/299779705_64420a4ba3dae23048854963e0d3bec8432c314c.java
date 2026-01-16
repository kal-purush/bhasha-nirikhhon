package com.example.cmput301f20t18;

import android.content.Context;
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

import org.w3c.dom.Text;

import java.util.List;

/**
 * Custom RecyclerView Adapter for Book objects
 */
public class BorrowedRecyclerViewAdapter extends RecyclerView.Adapter<BorrowedRecyclerViewAdapter.BookViewHolder> {

    private final String TAG = "BorrowedRecViewAdapter";
    private Context context;
    private List<Book> bookList;

    public BorrowedRecyclerViewAdapter(Context context, List<Book> bookList) {
        this.context = context;
        this.bookList = bookList;
    }

    @Override
    public int getItemViewType(int position) {

        Book book = bookList.get(position);

        int status = book.getStatus();
        switch (status) {
            case Book.STATUS_AVAILABLE: return 0;
            case Book.STATUS_REQUESTED: return 1;
            case Book.STATUS_ACCEPTED: return 2;
            case Book.STATUS_BORROWED: return 3;
        }
        return -1;
    }

    @NonNull
    @Override
    public BorrowedRecyclerViewAdapter.BookViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {

        LayoutInflater inflater = LayoutInflater.from(context);

        switch (viewType) {
            case Book.STATUS_AVAILABLE:
                return new BookViewHolder(inflater.inflate(R.layout.card_mybooks_no_requests, null));
            case Book.STATUS_REQUESTED:
                return new BookViewHolder(inflater.inflate(R.layout.card_borrowed_requested, null));
            case Book.STATUS_ACCEPTED:
                return new BookViewHolder(inflater.inflate(R.layout.card_mybooks_pending, null));
            case Book.STATUS_BORROWED:
                return new BookViewHolder(inflater.inflate(R.layout.card_mybooks_lending, null));
            default: Log.d(TAG, "Error: Book status not found");
        }

        View view = inflater.inflate(R.layout.card_mybooks_no_requests, null);
        return new BookViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull BookViewHolder holder, int position) {

        Book book = bookList.get(position);

        //holder.imageView.setImageResource(R.drawable.default_cover);
        holder.textViewTitle.setText(book.getTitle());
        holder.textViewAuthor.setText(book.getAuthor());
        holder.textViewYear.setText(String.valueOf(book.getYear()));
        holder.textViewISBN.setText(String.valueOf(book.getISBN()));

        try {
            // These two TextViews will be null if the book status is "Available"
            holder.textViewUsername.setText("Username");
            holder.textViewUserDescription.setText("owner");
        } catch (Exception e) {
            Log.e(TAG, e.toString());
        }

        int status = book.getStatus();
        switch (status) {
            case Book.STATUS_AVAILABLE:
                // Books in Borrowed section should never have status "available"
                Log.e(TAG, "onBindViewHolder: \"Available\" status not allowed in borrowed section.");
                break;
            case Book.STATUS_REQUESTED:

                // User clicks the "Cancel request" button
                holder.buttonCancelRequest.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        AlertDialog dialog = (new AlertDialog.Builder(v.getContext())
                                .setTitle("Cancel request")
                                .setMessage("Are you sure you want to cancel your request?")
                                .setPositiveButton("Yes", new DialogInterface.OnClickListener() {
                                    @Override
                                    public void onClick(DialogInterface dialog, int which) {
                                    }
                                })
                                .setNegativeButton("No", null)).show();

                        Button buttonPositive = dialog.getButton(DialogInterface.BUTTON_POSITIVE);
                        buttonPositive.setTextColor(ContextCompat.getColor(v.getContext(), R.color.colorPrimaryDark));
                        Button buttonNegative = dialog.getButton(DialogInterface.BUTTON_NEGATIVE);
                        buttonNegative.setTextColor(ContextCompat.getColor(v.getContext(), R.color.colorPrimaryDark));
                    }
                });

                // User clicks on profile photo
                holder.buttonUser.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        Toast.makeText(context, "*opens user profile* OwO", Toast.LENGTH_SHORT).show();
                        // TODO: open the users profile in a new activity
                    }
                });

                // User clicks on map button
                holder.buttonMap.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        Toast.makeText(context, "*View pick up location*", Toast.LENGTH_SHORT).show();
                        // Are we allowing the borrower to see the location before being accepted?
                        // TODO: make activity that displays pick up location to borrower
                    }
                });
                break;
            case Book.STATUS_ACCEPTED:

                // User clicks the "Confirm pick up" button
                holder.buttonConfirmPickUp.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        // The book isbn needs to be passed to Scanner
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
                        Toast.makeText(context, "*opens user profile* OwO", Toast.LENGTH_SHORT).show();
                        // TODO: open the users profile in a new activity
                    }
                });

                // User clicks on map button
                holder.buttonMap.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        Toast.makeText(context, "*View pick up location*", Toast.LENGTH_SHORT).show();
                        // TODO: make activity that displays pick up location to borrower
                    }
                });
                break;
            case Book.STATUS_BORROWED:

                // User clicks the "Confirm return" button
                holder.buttonConfirmReturn.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        // The book isbn needs to be passed to Scanner
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
                        Toast.makeText(context, "*opens user profile* OwO", Toast.LENGTH_SHORT).show();
                        // TODO: open the users profile in a new activity
                    }
                });

                // User clicks on map button
                holder.buttonMap.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        Toast.makeText(context, "*View pick up location*", Toast.LENGTH_SHORT).show();
                        // TODO: make activity that displays pick up location to borrower
                    }
                });
                break;
        }
    }

    @Override
    public int getItemCount() {
        return bookList.size();
    }

    public static class BookViewHolder extends RecyclerView.ViewHolder {

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

        public BookViewHolder(@NonNull View itemView) {
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

        }
    }
}