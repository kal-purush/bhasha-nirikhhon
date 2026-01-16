package app.ticketing.td.movieticketingsystem.activities;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.Spinner;
import android.widget.TextView;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import app.ticketing.td.movieticketingsystem.ConnectionClass;
import app.ticketing.td.movieticketingsystem.R;
import app.ticketing.td.movieticketingsystem.models.Movie;

public class MoviesActivity extends AppCompatActivity {

    private TextView TextView_AvailableMovies;
    private Spinner DropDown_Movies;
    private Button Button_Back;
    private Button Button_Next;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_movies);

        Intent intent = getIntent();
        String selectedCinema = intent.getStringExtra(MainActivity.SELECTED_CINEMA_NAME);
        int selectedCinemaId = intent.getIntExtra(MainActivity.SELECTED_CINEMA_ID, 0);
        //section for finding controls
        TextView_AvailableMovies = (TextView)findViewById(R.id.TextView_AvailableMovies);
        DropDown_Movies = (Spinner) findViewById(R.id.DropDown_Movies);
        Button_Back = (Button)findViewById(R.id.Button_Back);
        Button_Next = (Button)findViewById(R.id.Button_Next);

        //rest of the code
        Button_Back.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Intent intent = new Intent(MoviesActivity.this, MainActivity.class);
                startActivity(intent);
                finish();
            }
        });

        Button_Next.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {

            }
        });

        TextView_AvailableMovies.setText(TextView_AvailableMovies.getText() + " " + selectedCinema);
        List<Movie> movies = getMoviesFromDatabase(selectedCinemaId);
        if(movies.size() > 0) {
            ArrayAdapter<Movie> adapter = new ArrayAdapter<Movie>(MoviesActivity.this, android.R.layout.simple_spinner_dropdown_item, movies);
            adapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);
            DropDown_Movies.setAdapter(adapter);

            DropDown_Movies.setOnItemSelectedListener(new AdapterView.OnItemSelectedListener() {
                @Override
                public void onItemSelected(AdapterView<?> adapterView, View view, int i, long l) {

                }

                @Override
                public void onNothingSelected(AdapterView<?> adapterView) {

                }
            });
        }
        else {
            TextView_AvailableMovies.setText("No available movies for cinema " + selectedCinema);
            DropDown_Movies.setVisibility(View.INVISIBLE);
        }
    }

    private List<Movie> getMoviesFromDatabase(int id) {
        ArrayList<Movie> movies = new ArrayList<Movie>();
        //query example
        Statement statement = ConnectionClass.GetStatement();
        if (statement == null)
            return null;
        try {
            String query = "SELECT * FROM Movies WHERE IDCinema = " + id;
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                Movie movie = new Movie();
                movie.setID(resultSet.getInt(1));
                movie.setName(resultSet.getString(2));
                movie.setIDCinema(resultSet.getInt(3));
                movie.setOra(resultSet.getTime(4));
                movie.setData(resultSet.getDate(5));
                movie.setRoom(resultSet.getInt(6));

                movies.add(movie);
            }
            resultSet.close();
            statement.getConnection().close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return  movies;
    }

}