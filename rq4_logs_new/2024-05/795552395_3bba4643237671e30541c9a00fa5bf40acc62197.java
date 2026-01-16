package com.hyep.movietracker.adapter;

import android.content.Context;
import android.view.View;
import android.view.LayoutInflater;
import android.view.ViewGroup;
import android.widget.ImageButton;
import android.widget.ImageView;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.bumptech.glide.Glide;
//import com.bumptech.glide.RequestManager;
import com.hyep.movietracker.R;
import com.hyep.movietracker.models.Movie;
import com.hyep.movietracker.utils.Utils;

import java.util.List;

public class CardMovieAdapter extends RecyclerView.Adapter<CardMovieAdapter.CardMovieHolder> {

//    RequestManager glide;

    private final List<Movie> movieList;
    private final Context cont;

    public CardMovieAdapter(List<Movie> movieList, Context cont) {
        this.movieList = movieList;
        this.cont = cont;
    }

//    public CardMovieAdapter(RequestManager glide, List<Movie> movieList) {
//        this.glide = glide;
//        this.movieList = movieList;
//    }

    @NonNull
    @Override
    public CardMovieAdapter.CardMovieHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.list_movie_in_space,parent,false);
        return new CardMovieHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull CardMovieAdapter.CardMovieHolder holder, int position) {
        Movie movie = movieList.get(position);
        if(movie == null){
            return;
        }
        Glide.with(cont).load(Utils.BASE_IMG_URL+movieList.get(position).getPosterPath()).into(holder.imgPoster);
        holder.tvItemTitle.setText(movieList.get(position).getTitle());
        holder.imgBtnItemSetting.setOnClickListener(view -> Toast.makeText(cont, movie.getId()+"", Toast.LENGTH_SHORT).show());
    }

    @Override
    public int getItemCount() {
        if (movieList != null) {
            return movieList.size();
        }
        return 0;
    }

    public static class CardMovieHolder extends RecyclerView.ViewHolder {

        private final ImageView imgPoster;

        private final TextView tvItemTitle;

        private final ImageButton imgBtnItemSetting;


        public CardMovieHolder(View viewItem) {
            super(viewItem);
            imgPoster = viewItem.findViewById(R.id.imgPoster);
            tvItemTitle = viewItem.findViewById(R.id.tvItemTitle);
            imgBtnItemSetting = viewItem.findViewById(R.id.imgBtnItemSetting);


        }
    }


}