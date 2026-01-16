package com.example.cinemaapias.adapters;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.example.cinemaapias.R;
import com.example.cinemaapias.models.Movies;

import java.util.List;

public class MoviesAdapter extends RecyclerView.Adapter<MoviesAdapter.MoviesViewHolder> {

    private Context mCtx;
    private List<Movies> moviesList;

    public MoviesAdapter(Context mCtx, List<Movies> moviesList) {
        this.mCtx = mCtx;
        this.moviesList = moviesList;
    }

    @NonNull
    @Override
    public MoviesViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(mCtx).inflate(R.layout.recyclerview_movies, parent, false);
        return new MoviesViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull MoviesViewHolder holder, int position) {
        Movies movies= moviesList.get(position);

        holder.textViewName.setText(movies.getName());
        holder.textViewDescription.setText(movies.getDescription());
    }

    @Override
    public int getItemCount() {
        return moviesList.size();
    }

    class MoviesViewHolder extends RecyclerView.ViewHolder{

        TextView textViewName, textViewDescription;

        public MoviesViewHolder(@NonNull View itemView) {
            super(itemView);

            textViewName = itemView.findViewById(R.id.textViewName);
            textViewDescription = itemView.findViewById(R.id.textViewDescription);
        }
    }
}