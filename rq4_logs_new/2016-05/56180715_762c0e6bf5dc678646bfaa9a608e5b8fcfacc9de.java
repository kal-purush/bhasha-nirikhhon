package app.ticketing.td.movieticketingsystem.activities;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;

import java.util.ArrayList;

import app.ticketing.td.movieticketingsystem.R;
import app.ticketing.td.movieticketingsystem.models.Cinema;
import app.ticketing.td.movieticketingsystem.models.Movie;
import app.ticketing.td.movieticketingsystem.models.MovieDate;
import app.ticketing.td.movieticketingsystem.models.MovieTime;
import app.ticketing.td.movieticketingsystem.models.Seat;

public class ConfirmationActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_confirmation);

        Intent intent = getIntent();
        ArrayList<Seat> selectedSeats = intent.getParcelableArrayListExtra(SeatsActivity.SELECTED_SEATS);
        Cinema selectedCinema = intent.getParcelableExtra(MainActivity.SELECTED_CINEMA);
        Movie selectedMovie = intent.getParcelableExtra(MoviesActivity.SELECTED_MOVIE);
        MovieDate selectedDate = intent.getParcelableExtra(MoviesActivity.SELECTED_DATE);
        MovieTime selectedTime = intent.getParcelableExtra(MoviesActivity.SELECTED_TIME);
    }
}