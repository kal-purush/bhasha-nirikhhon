package com.hyep.movietracker.adapter;

import android.text.format.DateFormat;
import android.view.LayoutInflater;
import android.view.ViewGroup;

import androidx.annotation.NonNull;
import androidx.paging.PagingDataAdapter;
import androidx.recyclerview.widget.DiffUtil;
import androidx.recyclerview.widget.RecyclerView;

import com.bumptech.glide.RequestManager;
import com.hyep.movietracker.databinding.ListUpcomingMoviesBinding;
import com.hyep.movietracker.models.Movie;

import kotlin.coroutines.CoroutineContext;

public class MovieAdapter extends PagingDataAdapter<Movie, MovieAdapter.MovieViewHolder> {
    public static final int LOADING_ITEM = 0;
    public static final int MOVIE_ITEM = 1;

    String dateTimeFormat = "MMM dd, yyyy";
    RequestManager glide;

    public MovieAdapter(@NonNull DiffUtil.ItemCallback<Movie> diffCallback, @NonNull CoroutineContext mainDispatcher, RequestManager glide) {
        super(diffCallback, mainDispatcher);
        this.glide = glide;
    }

    @NonNull
    @Override
    public MovieViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        return new MovieViewHolder(ListUpcomingMoviesBinding.inflate(
                LayoutInflater.from(parent.getContext()), parent, false)
        );
    }

    @Override
    public int getItemViewType(int position) {
        return position == getItemCount() ? MOVIE_ITEM : LOADING_ITEM;
    }

    @Override
    public void onBindViewHolder(@NonNull MovieViewHolder holder, int position) {
        Movie currentMovie = getItem(position);
        if (currentMovie != null) {
            glide.load("https://image.tmdb.org/t/p/w500" + currentMovie.getPosterPath())
                .into(holder.moviesBinding.imgPoster);
            holder.moviesBinding.tvGenre.setText("Movie");
            holder.moviesBinding.tvTitle.setText(String.valueOf(currentMovie.getTitle()));
            holder.moviesBinding.tvDate.setText(String.valueOf(DateFormat.format(dateTimeFormat, currentMovie.getReleaseDate())));
        }



    }

    public class MovieViewHolder extends RecyclerView.ViewHolder {
        ListUpcomingMoviesBinding moviesBinding;
        public MovieViewHolder(@NonNull ListUpcomingMoviesBinding moviesBinding) {
            super(moviesBinding.getRoot());

            this.moviesBinding = moviesBinding;
        }
    }
}