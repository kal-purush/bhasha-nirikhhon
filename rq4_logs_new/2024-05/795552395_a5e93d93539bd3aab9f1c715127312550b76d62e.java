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
import com.hyep.movietracker.models.Movie;
import com.hyep.movietracker.models.SearchModel;
import com.hyep.movietracker.utils.Utils;

import java.util.List;

public class SearchAdapter extends  RecyclerView.Adapter<SearchAdapter.SearchViewHolder> {
    private final List<SearchModel> list;
    private Context con;

    String dateTimeFormat = "MMM dd, yyyy";
    public SearchAdapter(List<SearchModel> list, Context con) {
        this.list = list;
        this.con = con;
    }

    @NonNull
    @Override
    public SearchAdapter.SearchViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.card_item, parent, false);
        return new SearchAdapter.SearchViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull SearchAdapter.SearchViewHolder holder, int position) {
        SearchModel item = list.get(position);
        if (item == null) {
            return;
        }
        if (item.getMediaType() == "movie") {
            Glide.with(con).load(Utils.BASE_IMG_URL + item.getPosterPath()).into(holder.imgPoster);
            holder.tvTitle.setText(item.getTitle());
            holder.tvGenre.setText(item.getMediaType());
            holder.tvReleaseDate.setText(String.valueOf(DateFormat.format(dateTimeFormat, item.getReleaseDate())));
        } else if (item.getMediaType() == "tv") {
            Glide.with(con).load(Utils.BASE_IMG_URL + item.getPosterPath()).into(holder.imgPoster);
            holder.tvTitle.setText(item.getName());
            holder.tvGenre.setText(item.getMediaType());
            holder.tvReleaseDate.setText(String.valueOf(DateFormat.format(dateTimeFormat, item.getReleaseDate())));
        }
        else {
            return;
        }

    }

    @Override
    public int getItemCount() {
        if (list != null) {
            return list.size();
        }
        return 0;
    }


    public static class SearchViewHolder extends RecyclerView.ViewHolder {

        private TextView tvTitle;
        private TextView tvReleaseDate;
        private  TextView tvGenre;
        private ImageView imgPoster;

        public SearchViewHolder(@NonNull View itemView) {
            super(itemView);
            tvTitle = itemView.findViewById(R.id.tvTitle);
            tvReleaseDate = itemView.findViewById(R.id.tvDate);
            tvGenre = itemView.findViewById(R.id.tvGenre);
            imgPoster = itemView.findViewById(R.id.imagePoster);
        }
    }
}