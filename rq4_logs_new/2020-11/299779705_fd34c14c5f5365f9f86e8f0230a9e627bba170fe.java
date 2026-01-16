package com.example.cmput301f20t18;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

public class ImageRecyclerViewAdapter extends RecyclerView.Adapter<ImageRecyclerViewAdapter.ImageViewHolder> {

    // add the variable container that are need for the images and create a constructor

    @NonNull
    @Override
    public ImageViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {

        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.image_single_view,parent,false);
        ImageViewHolder imageHolder = new ImageViewHolder(view);
        return imageHolder;
    }

    @Override
    public void onBindViewHolder(@NonNull ImageViewHolder holder, int position) {
        //holder.bookImage.setImageResource(); This is to set to the image resource
    }

    @Override
    public int getItemCount() {
        return 0; // This needs to be set to the length of the container
    }

    public class ImageViewHolder extends RecyclerView.ViewHolder{

        ImageView bookImage;

        public ImageViewHolder(@NonNull View itemView) {
            super(itemView);
            bookImage = itemView.findViewById(R.id.book_image_view);
        }
    }
}