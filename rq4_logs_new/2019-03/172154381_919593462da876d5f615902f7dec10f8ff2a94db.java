package com.example.musicroulette;

import android.os.AsyncTask;
import android.util.Log;

import com.example.musicroulette.utils.NetworkUtils;

import java.io.IOException;

public class SpotifyAuthorizationTask extends AsyncTask<String, Void, String> {

    private static final String TAG = SpotifyAuthorizationTask.class.getSimpleName();

    @Override
    protected String doInBackground(String... params) {
        String url = params[0];
        String results = null;
        try {
            results = NetworkUtils.doHTTPGet(url);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return results;
    }

    @Override
    protected void onPostExecute(String s) {
        if(s != null) {
            Log.d(TAG, "Results from Authentication: " + s);
        }
    }
}