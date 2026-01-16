package com.ckt.mikel.chicktionary;

import android.support.v7.app.ActionBar;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.widget.ImageView;
import android.widget.RatingBar;
import android.widget.TextView;

public class DetailActivity extends AppCompatActivity {
    private ImageView imageViewDetailedImage;
    private TextView textViewDetailedName;
    private TextView textViewEmail;
    private TextView textViewPhone;
    private TextView textViewDetailedMark;
    private RatingBar ratingBar;
    private Person person;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_detail);


        ActionBar actionBar = getSupportActionBar();

        if (actionBar != null) {
            actionBar.setDisplayHomeAsUpEnabled(true);
        }


        //imageViewDetailedImage = (ImageView) findViewById(R.id.imageViewDetailedPerson);

        textViewDetailedName = (TextView) findViewById(R.id.textViewDetailedName);
        textViewEmail = (TextView) findViewById(R.id.textViewEmail);
        textViewPhone = (TextView) findViewById(R.id.textViewPhone);
        textViewDetailedMark = (TextView) findViewById(R.id.textViewDetailedMark);
        ratingBar = (RatingBar) findViewById(R.id.ratingBar);
        ratingBar.setNumStars(10);

        person = (Person) getIntent().getSerializableExtra("persona");

        if (person != null) {
            textViewDetailedName.setText(person.getName());
            textViewEmail.setText(person.getEmail());
            textViewPhone.setText(person.getPhone());
            textViewDetailedMark.setText(String.valueOf(person.getRating()));
            ratingBar.setRating(person.getRating());
        }
    }

}