package app.ticketing.td.movieticketingsystem.activities;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;

import app.ticketing.td.movieticketingsystem.R;
import app.ticketing.td.movieticketingsystem.models.Movie;
import app.ticketing.td.movieticketingsystem.models.MovieDate;
import app.ticketing.td.movieticketingsystem.models.MovieTime;

public class SeatsActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_seats);

        Intent intent = getIntent();
//      Movie someMovie = intent.getParcelableExtra(MoviesActivity.SELECTED_MOVIE);
//      MovieDate selectedDate = intent.getParcelableExtra(MoviesActivity.SELECTED_DATE);
//
//      MovieTime selectedTime = intent.getParcelableExtra(MoviesActivity.SELECTED_TIME);
    }
}