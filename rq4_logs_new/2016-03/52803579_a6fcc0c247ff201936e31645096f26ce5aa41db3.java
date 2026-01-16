package com.udacity.gradle.builditbigger;

import android.content.Context;
import android.os.AsyncTask;
import android.util.Log;
import android.widget.Toast;

import com.gropoid.bigjoke.backend.jokeApi.JokeApi;
import com.gropoid.bigjoke.backend.jokeApi.model.JokeBean;
import com.gropoid.jokelibrary.JokeDisplayActivity;

import java.io.IOException;

public class FetchJokeTask extends AsyncTask<Context, Void, String> {
    private Context mContext;
    private final String TAG = FetchJokeTask.class.getSimpleName();

    @Override
    protected String doInBackground(Context... params) {
        if(params.length != 1) {
            return null;
        }
        mContext = params[0];
        JokeApi api = JokeResource.getJokeApi();
        try {
            JokeBean joke = api.getJoke().execute();
            return joke.getJoke();
        } catch (IOException ioe) {
            Log.e(TAG, "IOException : " + ioe.toString());
        }
        return null;
    }

    @Override
    protected void onPostExecute(String jokeString) {
        if (jokeString != null) {
            JokeDisplayActivity.startWithJoke(mContext, jokeString);
        } else {
            Toast.makeText(mContext, "Could not retrieve joke :o(", Toast.LENGTH_SHORT).show();
        }
    }
}
       