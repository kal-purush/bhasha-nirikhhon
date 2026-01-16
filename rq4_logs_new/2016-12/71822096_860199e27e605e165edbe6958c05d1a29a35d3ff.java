package com.example.hisaki.netflix.adapters;

import android.app.FragmentManager;
import android.view.View;
import android.widget.AdapterView;

import com.example.hisaki.netflix.R;
import com.example.hisaki.netflix.enteties.Movie;
import com.example.hisaki.netflix.fragments.MovieContent;

import java.util.List;

/**
 * Created by hisaki on 12.12.2016.
 */

public class MovieClickListener implements AdapterView.OnItemClickListener {
    FragmentManager fragmentManager;
    List<Movie> movies;
    boolean menu_enable;

    public MovieClickListener(FragmentManager fragmentManager, List<Movie> movies,boolean menu_enable){
        this.fragmentManager = fragmentManager;
        this.movies = movies;
        this.menu_enable = menu_enable;
    }
    public void setMovies(List<Movie> movies){
        this.movies = movies;
    }

    public void onItemClick(AdapterView<?> adapterView, View view, int position, long id) {
        android.app.FragmentTransaction transaction = fragmentManager.beginTransaction();
        MovieContent movie_fragment  = new MovieContent();
        movie_fragment.setHasOptionsMenu(menu_enable);
        movie_fragment.setMovie(movies.get(position));
        transaction.replace(R.id.content,movie_fragment);
        transaction.addToBackStack("movie");
        transaction.commit();
    }
}