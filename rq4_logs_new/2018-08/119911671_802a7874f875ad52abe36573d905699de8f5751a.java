package com.example.android.popularmovies.loaders;

import android.content.Context;
import android.support.v4.content.AsyncTaskLoader;
import android.util.Log;

import com.example.android.popularmovies.DetailActivity;
import com.example.android.popularmovies.utilities.MovieDBJsonUtils;
import com.example.android.popularmovies.utilities.NetworkUtils;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;

public class ReviewLoader extends AsyncTaskLoader<ArrayList<HashMap<String, String>>> {
    private static final String TAG = DetailActivity.class.getSimpleName();
    private ArrayList<HashMap<String, String>> mReviewData;
    private String mApiId;

    public ReviewLoader(Context context, String movieApiId) {
        super(context);
        this.mApiId = movieApiId;
    }

    @Override
    protected void onStartLoading() {
        if (mReviewData == null) {
            forceLoad();
        }
    }

    @Override
    public ArrayList<HashMap<String, String>> loadInBackground() {
        String jsonMoviesResponse;
        ArrayList<HashMap<String, String>> movieDataArrayList;

        try {
            URL movieRequestUrl = NetworkUtils.buildDetailURL(mApiId, "reviews");
            Log.d(TAG, "Reviews url: " + movieRequestUrl);
            jsonMoviesResponse = NetworkUtils.getResponseFromHttpUrl(movieRequestUrl);
            Log.d(TAG, "Reviews json resp: " + jsonMoviesResponse);
            movieDataArrayList = MovieDBJsonUtils.getReviewDataFromJson(getContext(), jsonMoviesResponse);
            return movieDataArrayList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void deliverResult(ArrayList<HashMap<String, String>> deliveredValues) {
        mReviewData = deliveredValues;
        Log.d(TAG, "reviews result delivered");
        if (deliveredValues == null) {
            Log.d(TAG, "reviews values are null");
            return;
        }
        super.deliverResult(deliveredValues);
    }
}