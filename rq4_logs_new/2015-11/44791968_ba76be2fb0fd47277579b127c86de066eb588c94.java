package com.jefftiensivu.popularmovies.data;

import android.app.Activity;
import android.content.ContentProviderOperation;
import android.content.OperationApplicationException;
import android.database.Cursor;
import android.net.Uri;
import android.os.RemoteException;
import android.util.Log;

import com.jefftiensivu.popularmovies.model.MovieInfo;
import com.jefftiensivu.popularmovies.utility.Time;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jeff on 11/3/2015.
 * Not an SQLiteOpenHelper.
 */
public class PopMoviesDbHelper {
    private static final String LOG_TAG = PopMoviesDbHelper.class.getSimpleName();


    public static void insertData(List<MovieInfo> movieArray, String sort, Activity parentActivity){
        Log.d(LOG_TAG, "insert");
        ArrayList<ContentProviderOperation> batchOperations = new ArrayList<>(movieArray.size());

        Cursor c;
        Uri contentUri;
        switch (sort) {
            case PopMoviesColumns.POPULARITY:
                contentUri = PopMoviesProvider.PopularMovies.CONTENT_URI;
                c = parentActivity.getContentResolver().query(
                        contentUri, null, null, null, null);
                break;
            case PopMoviesColumns.VOTE_AVERAGE:
                contentUri = PopMoviesProvider.HighestRatedMovies.CONTENT_URI;
                c = parentActivity.getContentResolver().query(
                        contentUri, null, null, null, null);
                break;
//            case PopMoviesColumns.FAV:
            default:
                contentUri = PopMoviesProvider.FavoriteMovies.CONTENT_URI;
                c = parentActivity.getContentResolver().query(
                        contentUri, null, null, null, null);
        }

        if (c == null || c.getCount() == 0) {
            for (MovieInfo movieInfo : movieArray) {
                ContentProviderOperation.Builder builder = ContentProviderOperation.newInsert(
                        contentUri);
                builder.withValue(PopMoviesColumns.TMDB_ID, movieInfo.getId());
                builder.withValue(PopMoviesColumns.TITLE, movieInfo.getTitle());
                Log.i(LOG_TAG, "Inserting data for " + movieInfo.getTitle()); ///////////////////is this happening???
                builder.withValue(PopMoviesColumns.OVERVIEW, movieInfo.getOverview());
                builder.withValue(PopMoviesColumns.RELEASE_DATE, movieInfo.getReleaseDate());
                builder.withValue(PopMoviesColumns.POSTER_PATH, movieInfo.getUrl());
                builder.withValue(PopMoviesColumns.POPULARITY, movieInfo.getPopularity());
                builder.withValue(PopMoviesColumns.VOTE_AVERAGE, movieInfo.getVoteAverage());
                builder.withValue(PopMoviesColumns.VOTE_COUNT, movieInfo.getVoteCount());
                builder.withValue(PopMoviesColumns.TIME_STAMP, Time.getTimeStamp());
                batchOperations.add(builder.build());
            }
            try {
                parentActivity.getContentResolver().applyBatch(PopMoviesProvider.AUTHORITY, batchOperations);
            } catch (RemoteException | OperationApplicationException e) {
                Log.e(LOG_TAG, "Error applying batch insert", e);
            }
        }
        c.close();
    }

/*
    public void addData(){
        Log.d(LOG_TAG, "Adding Data");
        ArrayList<ContentProviderOperation> batchOperations = new ArrayList<>(movieArray.size());

        for(MovieInfo movieInfo : movieArray){
//            if(movieInfo.getId() == )
            ContentProviderOperation.Builder builder = ContentProviderOperation.newInsert(
                    PopMoviesProvider.PopularMovies.CONTENT_URI);
            builder.withValue(PopMoviesColumns.TMDB_ID, movieInfo.getId());
            builder.withValue(PopMoviesColumns.TITLE, movieInfo.getTitle());
            Log.i(LOG_TAG, "Adding data for " + movieInfo.getTitle()); ///////////////////is this happening???
            builder.withValue(PopMoviesColumns.OVERVIEW, movieInfo.getOverview());
            builder.withValue(PopMoviesColumns.RELEASE_DATE, movieInfo.getReleaseDate());
            builder.withValue(PopMoviesColumns.POSTER_PATH, movieInfo.getUrl());
            builder.withValue(PopMoviesColumns.POPULARITY, movieInfo.getPopularity());
            builder.withValue(PopMoviesColumns.VOTE_AVERAGE, movieInfo.getVoteAverage());
            builder.withValue(PopMoviesColumns.VOTE_COUNT, movieInfo.getVoteCount());
            batchOperations.add(builder.build());
        }

        try{
            getActivity().getContentResolver().applyBatch(PopMoviesProvider.AUTHORITY, batchOperations);
        }catch(RemoteException | OperationApplicationException e){
            Log.e(LOG_TAG, "Error applying batch insert", e);
        }
    }
*/
}