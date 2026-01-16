package com.example.hisaki.netflix.fragments;

import android.app.Fragment;
import android.content.Context;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.GridView;
import android.widget.LinearLayout;
import android.widget.ListView;

import com.example.hisaki.netflix.R;
import com.example.hisaki.netflix.adapters.MovieAdapter;
import com.example.hisaki.netflix.enteties.Movie;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hisaki on 25.10.2016.
 */

public class SavedMovies extends Fragment {
    MovieAdapter adapter;
    List<Movie> movies;
    @Nullable
    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        View layout = inflater.inflate(R.layout.movies_list_fragment,container,false);
        GridView list = (GridView) layout.findViewById(R.id.list);
        View edit = layout.findViewById(R.id.option);
        edit.setVisibility(View.INVISIBLE);
        edit.setLayoutParams(new LinearLayout.LayoutParams(1,1));
        movies = new ArrayList<>();
        adapter = new MovieAdapter(getActivity(),R.layout.movie_item_fragment,movies);

        list.setAdapter(adapter);
        return layout;
    }


    public void setAdapter(){
        movies = new ArrayList<>();
        Movie movie = new Movie();
        movie.setCategory("horror");
        movie.setDirector("tor");
        movie.setRating("5");
        movie.setReleaseYear("30.10.16");
        movie.setShowTitle("Horror");
        movies.add(movie);
    }
}