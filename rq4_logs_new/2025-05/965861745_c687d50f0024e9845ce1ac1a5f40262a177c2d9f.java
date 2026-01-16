package com.daclink.citypulse;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.bumptech.glide.Glide;
import com.daclink.citypulse.model.YelpBusiness;

import java.util.List;

public class YelpAdapter extends RecyclerView.Adapter<YelpAdapter.YelpViewHolder> {

    private final List<YelpBusiness> businesses;
    private final Context context;

    public YelpAdapter(List<YelpBusiness> businesses, Context context) {
        this.businesses = businesses;
        this.context = context;
    }

    @NonNull
    @Override
    public YelpViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(context).inflate(R.layout.item_yelp_business, parent, false);
        return new YelpViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull YelpViewHolder holder, int position) {
        YelpBusiness business = businesses.get(position);
        holder.nameTextView.setText(business.getName());
        holder.addressTextView.setText(business.getLocation().address1);
        holder.ratingTextView.setText("Rating: " + business.getRating());

        Glide.with(context)
                .load(business.getImageUrl())
                .into(holder.imageView);
    }

    @Override
    public int getItemCount() {
        return businesses.size();
    }

    public static class YelpViewHolder extends RecyclerView.ViewHolder {
        TextView nameTextView, addressTextView, ratingTextView;
        ImageView imageView;

        public YelpViewHolder(@NonNull View itemView) {
            super(itemView);
            nameTextView = itemView.findViewById(R.id.yelp_name);
            addressTextView = itemView.findViewById(R.id.yelp_address);
            ratingTextView = itemView.findViewById(R.id.yelp_rating);
            imageView = itemView.findViewById(R.id.yelp_image);
        }
    }
}