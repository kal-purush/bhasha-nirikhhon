package com.example.hisaki.netflix.adapters;

import android.content.Context;
import android.support.annotation.NonNull;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.ImageView;
import android.widget.TextView;

import com.example.hisaki.netflix.R;
import com.example.hisaki.netflix.enteties.Movie;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hisaki on 26.10.2016.
 */

public class MovieAdapter extends ArrayAdapter {
    List<Movie> movies;
    int listType;

    public MovieAdapter(Context context, int resource, List<Movie> movies) {
        super(context, resource);
        this.movies     = movies;
    }

    @Override
    public int getCount() {
        return movies.size();
    }

    @NonNull
    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        MovieHolder holder;
        if(convertView == null){

            LayoutInflater inflater = (LayoutInflater) getContext()
                    .getSystemService(Context.LAYOUT_INFLATER_SERVICE);
            convertView = inflater.inflate(R.layout.movie_item_fragment,parent,false);

            holder  = new MovieHolder(
                    convertView.findViewById(R.id.category),
                    convertView.findViewById(R.id.director),
                    convertView.findViewById(R.id.imageView),
                    convertView.findViewById(R.id.rating),
                    convertView.findViewById(R.id.release),
                    convertView.findViewById(R.id.titletle));
            convertView.setTag(holder);
        }
        else
            holder = (MovieHolder)convertView.getTag();

        Movie movie = movies.get(position);

        holder.category.setText(movie.getCategory());
        holder.director.setText(movie.getDirector());
        //holder.image.setImageBitmap(movie.getImage());
        holder.rating.setText(movie.getRating());
        holder.release.setText(movie.getReleaseYear());
        holder.title.setText(movie.getShowTitle());

        return convertView;
    }
}