package com.example.ebram.popmovies;

import android.annotation.TargetApi;
import android.content.Context;
import android.database.Cursor;
import android.os.Build;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.CursorAdapter;
import android.widget.ImageView;
import android.widget.TextView;

import com.squareup.picasso.Picasso;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by Ebram Samy on 9/15/2015.
 */
public class MovieCursorAdapter extends CursorAdapter {

    @TargetApi(Build.VERSION_CODES.HONEYCOMB)
    public MovieCursorAdapter(Context context, Cursor c, int flags) {
        super(context, c, flags);
    }


    @Override
    public View newView(Context context, Cursor cursor, ViewGroup parent) {
        View view = LayoutInflater.from(context).inflate(R.layout.grid_item, parent, false);

        return view;
    }


    @Override
    public void bindView(View view, Context context, Cursor cursor) {

        String title= cursor.getString(MainActivityFragment.COLUMN_MOVIE_TITLE);
        TextView movieTit = (TextView) view.findViewById(R.id.movieTitle);
        movieTit.setText(title);

        String postPath= cursor.getString(MainActivityFragment.COLUMN_MOVIE_POSTER_PATH);
        ImageView moviePost = (ImageView) view.findViewById(R.id.moviePoster);
        URL url= null;
        try {
            url = new URL(postPath);
            // new DownloadImageTask(holder.moviePost).execute(url.toString());
            Picasso.with(context).load(url.toString()).into(moviePost);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

    }

    public static class ViewHolder {

        public final TextView movieTit;

        public final ImageView moviePost;

        public ViewHolder(View view) {
            movieTit = (TextView) view.findViewById(R.id.movieTitle);
            moviePost = (ImageView) view.findViewById(R.id.moviePoster);
        }
    }
}