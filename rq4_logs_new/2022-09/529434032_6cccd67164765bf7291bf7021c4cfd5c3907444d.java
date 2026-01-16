package com.example.firebasegsocapp.adapter;

import android.view.View;
import android.widget.ImageView;
import android.widget.TextView;
import androidx.recyclerview.widget.RecyclerView;
import com.example.firebasegsocapp.R;

public class FileViewHolder extends RecyclerView.ViewHolder {

    public TextView nameTextView;
    public ImageView imageView;
    public TextView downloadView;
    public TextView fileSizeView;
    public TextView createdOnView;

    // We also create a constructor that accepts the entire item row
    // and does the view lookups to find each subview
    public FileViewHolder(View itemView) {
        // Stores the itemView in a public final member variable that can be used
        // to access the context from any ViewHolder instance.
        super(itemView);

        nameTextView = itemView.findViewById(R.id.txtName);
        imageView = itemView.findViewById(R.id.imageView);
        downloadView = itemView.findViewById(R.id.txtViewDownloadFile);
        fileSizeView = itemView.findViewById(R.id.txtViewFileSizeValue);
        createdOnView = itemView.findViewById(R.id.txtViewCreationTimeValue);
    }
}