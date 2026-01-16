package uk.co.pauldavies83.popularmovies;

import android.content.Context;
import android.content.Intent;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.support.v4.view.MenuItemCompat;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.GridLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.Spinner;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class MainActivity extends AppCompatActivity implements MovieGridAdapter.MovieClickListener {

    public static final String THEMOVIEDB_BASE_URL = "http://api.themoviedb.org/3/movie";
    public static final String MOST_POPULAR = "popular";
    public static final String TOP_RATED = "top_rated";

    public static final String MOVIE_PARCEL_KEY = "movie_parcel";

    private String apiKey;
    private OkHttpClient okHttpClient;

    private RecyclerView movieGrid;
    private View progressBar;

    private MovieGridAdapter movieGridAdapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        apiKey = getResources().getString(R.string.tmdb_v3_api_key);
        okHttpClient = new OkHttpClient();

        progressBar = findViewById(R.id.progress_bar);
        createMovieGridView();
        fetchMovieData(MOST_POPULAR);
    }

    private void fetchMovieData(String sortOrder) {
        new FetchMovieDataTask().execute(sortOrder);
    }

    private void createMovieGridView() {
        RecyclerView.LayoutManager gridLayout = new GridLayoutManager(this, 2);
        movieGrid = (RecyclerView) findViewById(R.id.rv_movie_grid);
        movieGrid.setLayoutManager(gridLayout);
        movieGridAdapter = new MovieGridAdapter(this, this);
        movieGrid.setAdapter(movieGridAdapter);
    }

    public boolean isOnline() {
        ConnectivityManager cm = (ConnectivityManager) getSystemService(Context.CONNECTIVITY_SERVICE);
        NetworkInfo netInfo = cm.getActiveNetworkInfo();
        return netInfo != null && netInfo.isConnectedOrConnecting();
    }

    @Override
    public void onMovieClicked(Movie movie) {
        Intent intent = new Intent(this, MovieDetailActivity.class);
        intent.putExtra(MOVIE_PARCEL_KEY, movie);
        startActivity(intent);
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        getMenuInflater().inflate(R.menu.sort_order, menu);

        MenuItem item = menu.findItem(R.id.sort_order_spinner);
        Spinner spinner = (Spinner) MenuItemCompat.getActionView(item);

        ArrayAdapter<CharSequence> adapter = ArrayAdapter.createFromResource(this, R.array.spinner_list_item_array, android.R.layout.simple_spinner_item);
        adapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);

        spinner.setAdapter(adapter);
        spinner.setOnItemSelectedListener(new AdapterView.OnItemSelectedListener() {
            @Override
            public void onItemSelected(AdapterView<?> adapterView, View view, int item, long l) {
                switch (item) {
                    case 0:
                        fetchMovieData(MOST_POPULAR);
                        break;
                    case 1:
                        fetchMovieData(TOP_RATED);
                        break;
                }
            }

            @Override
            public void onNothingSelected(AdapterView<?> adapterView) {}
        });

        return true;
    }


    class FetchMovieDataTask extends AsyncTask<String, Object, Movie[]> {
        @Override
        protected void onPreExecute() {
            movieGrid.setVisibility(View.GONE);
            progressBar.setVisibility(View.VISIBLE);
        }

        @Override
        protected Movie[] doInBackground(String... params) {
            String sortOrder;
            Movie[] movies = null;

            if (isOnline()) {
                if (params != null && params.length > 0) {
                    sortOrder = params[0];
                } else {
                    sortOrder = MOST_POPULAR;
                }

                Uri uri = Uri.parse(THEMOVIEDB_BASE_URL)
                        .buildUpon()
                        .appendPath(sortOrder)
                        .appendQueryParameter("api_key", apiKey)
                        .build();

                Request request = new Request.Builder()
                        .url(uri.toString())
                        .get()
                        .build();

                try {
                    Response response = okHttpClient.newCall(request).execute();
                    JSONObject jsonResponse = new JSONObject(response.body().string());
                    JSONArray results = jsonResponse.getJSONArray("results");
                    movies = new Movie[results.length()];

                    for (int i = 0; i < results.length(); i++) {
                        movies[i] = new Movie(results.getJSONObject(i));
                    }
                } catch (IOException | JSONException e) {
                    e.printStackTrace();
                }
            } else {
                movies = new Movie[0];
            }

            return movies;
        }

        @Override
        protected void onPostExecute(Movie[] movies) {
            if (movies != null) {
                movieGridAdapter.setMovies(movies);
                movieGrid.setVisibility(View.VISIBLE);
                progressBar.setVisibility(View.GONE);
            }
        }
    }



}
