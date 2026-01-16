package com.example.cmput301f20t18;

import android.content.Context;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import org.w3c.dom.Text;

import java.util.List;

/**
 * Custom RecyclerView Adapter
 */
public class BookAdapter extends RecyclerView.Adapter<BookAdapter.BookViewHolder> {

    private final String TAG = "BookAdapter";
    private Context context;
    private List<Book> bookList;

    public BookAdapter(Context context, List<Book> bookList) {
        this.context = context;
        this.bookList = bookList;
    }

    @Override
    public int getItemViewType(int position) {

        Book book = bookList.get(position);

        String status = book.getStatus();
        switch (status) {
            case "available": return 0;
            case "requested": return 1;
            case "accepted": return 2;
            case "borrowed": return 3;
        }
        return -1;
    }

    @NonNull
    @Override
    public BookAdapter.BookViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {

        LayoutInflater inflater = LayoutInflater.from(context);

        switch (viewType) {
            case 0: return new BookViewHolder(inflater.inflate(R.layout.card_mybooks_no_requests, null));
            case 1: return new BookViewHolder(inflater.inflate(R.layout.card_mybooks_requested, null));
            case 2: break; //TODO
            case 3: break; //TODO
            default: Log.d(TAG, "Error: Book status not found");
        }

        View view = inflater.inflate(R.layout.card_mybooks_no_requests, null);
        return new BookViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull BookViewHolder holder, int position) {

        Book book = bookList.get(position);

        holder.imageView.setImageResource(R.drawable.default_cover);
        holder.textViewTitle.setText(book.getTitle());
        holder.textViewAuthor.setText(book.getAuthor());
        //holder.textViewYear.setText(book.getYear());
        holder.textViewISBN.setText(Long.toString(book.getISBN()));
        holder.textViewStatus.setText(book.getStatus());

    }

    @Override
    public int getItemCount() {
        return bookList.size();
    }

    public class BookViewHolder extends RecyclerView.ViewHolder {

        ImageView imageView;
        TextView textViewTitle;
        TextView textViewAuthor;
        TextView textViewYear;
        TextView textViewISBN;
        TextView textViewStatus;

        public BookViewHolder(@NonNull View itemView) {
            super(itemView);

            imageView = itemView.findViewById(R.id.image_view);
            textViewTitle = itemView.findViewById(R.id.text_book_title);
            textViewAuthor = itemView.findViewById(R.id.text_book_author);
            textViewYear = itemView.findViewById(R.id.text_book_year);
            textViewISBN = itemView.findViewById(R.id.text_book_isbn);

            // temporary
            textViewStatus = itemView.findViewById(R.id.text_book_status);

        }
    }
}