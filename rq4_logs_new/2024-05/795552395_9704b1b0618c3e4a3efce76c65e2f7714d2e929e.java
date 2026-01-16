package com.hyep.movietracker.adapter;

import android.content.Context;
import android.text.format.DateFormat;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.bumptech.glide.Glide;
import com.hyep.movietracker.R;
import com.hyep.movietracker.models.TV;
import com.hyep.movietracker.utils.Utils;

import java.util.List;

public class DiscoverTVAdapter extends RecyclerView.Adapter<DiscoverTVAdapter.CardViewHolder> {
    private final List<TV> tvList;
    private Context con;

    String dateTimeFormat = "MMM dd, yyyy";
    public DiscoverTVAdapter(List<TV> tvList, Context con) {
        this.tvList = tvList;
        this.con = con;
    }

    @NonNull
    @Override
    public DiscoverTVAdapter.CardViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.card_item, parent, false);
        return new DiscoverTVAdapter.CardViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull DiscoverTVAdapter.CardViewHolder holder, int position) {
        TV tv = tvList.get(position);
        if (tv == null) {
            return;
        }
        Glide.with(con).load(Utils.BASE_IMG_URL + tvList.get(position).getPosterPath()).into(holder.imgPoster);
        holder.tvName.setText(tvList.get(position).getName());
        holder.tvGenre.setText("TV Show");
        holder.tvReleaseDate.setText(String.valueOf(DateFormat.format(dateTimeFormat, tvList.get(position).getReleaseDate())));
    }

    @Override
    public int getItemCount() {
        if (tvList != null) {
            return tvList.size();
        }
        return 0;
    }

    public static class CardViewHolder extends RecyclerView.ViewHolder {

        private TextView tvName;
        private TextView tvReleaseDate;
        private  TextView tvGenre;
        private ImageView imgPoster;


        public CardViewHolder(@NonNull View itemView) {
            super(itemView);
            tvName = itemView.findViewById(R.id.tvTitle);
            tvReleaseDate = itemView.findViewById(R.id.tvDate);
            tvGenre = itemView.findViewById(R.id.tvGenre);
            imgPoster = itemView.findViewById(R.id.imagePoster);
        }
    }
}