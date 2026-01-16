package com.dl.dlexerciseandroid.background.service;

import android.app.IntentService;
import android.content.Context;
import android.content.Intent;
import android.text.TextUtils;
import android.util.Log;

import com.dl.dlexerciseandroid.utility.utils.HttpUtils;
import com.dl.dlexerciseandroid.utility.utils.MovieUrlUtils;

import java.io.IOException;

/**
 * Created by logicmelody on 2016/7/29.
 */
public class MovieInfoService extends IntentService {

    private static final String TAG = MovieInfoService.class.getName();

    public static final class Actions {
        public static final String SEARCH_MOVIES_BY_TEXT = "com.dl.dlexerciseandroid.ACTION_SEARCH_MOVIES_BY_TEXT";
    }

    public static final class ExtraKeys {
        public static final String STRING_QUERY_TEXT = "com.dl.dlexerciseandroid.EXTRA_QUERY_TEXT";
    }


    public static Intent generateSearchMoviesByTextIntent(Context context, String queryText) {
        Intent intent = new Intent(context, MovieInfoService.class);
        intent.setAction(Actions.SEARCH_MOVIES_BY_TEXT);
        intent.putExtra(ExtraKeys.STRING_QUERY_TEXT, queryText);

        return intent;
    }

    public MovieInfoService() {
        super(TAG);
    }

    @Override
    protected void onHandleIntent(Intent intent) {
        String action = intent.getAction();

        try {
            if (Actions.SEARCH_MOVIES_BY_TEXT.equals(action)) {
                searchMoviesByText(intent);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void searchMoviesByText(Intent intent) throws IOException {
        String queryText = intent.getStringExtra(ExtraKeys.STRING_QUERY_TEXT);

        if (TextUtils.isEmpty(queryText)) {
            return;
        }

        String url = MovieUrlUtils.searchMoviesByTextUrl(queryText);
        String jsonString = HttpUtils.getJsonStringFromUrl(url);

        Log.d("danny", "Movie search Url string = " + url);
        Log.d("danny", "Movie search Json string = " + jsonString);
    }
}