package com.example.backpackerlk.Activities;

import static com.example.backpackerlk.R.id.bottom_navigation;

import android.animation.ObjectAnimator;
import android.annotation.SuppressLint;
import android.content.Intent;
import android.os.Bundle;
import android.widget.ImageView;
import androidx.appcompat.app.AppCompatActivity;
import androidx.cardview.widget.CardView;

import com.example.backpackerlk.R;
import com.google.android.material.bottomnavigation.BottomNavigationView;

public class WhoAreYou2 extends AppCompatActivity {

    private CardView travelerCard, sellerCard;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_who_are_you); // Use your layout XML

        //initialize the card views
        travelerCard = findViewById(R.id.travelerCard);
        sellerCard = findViewById(R.id.sellerCard);

        //set click listners for each card views
        travelerCard.setOnClickListener(view -> navigateToHome());
        sellerCard.setOnClickListener(view -> navigateToHome());

        // Find the ImageView by ID
        ImageView imageView = findViewById(R.id.imageView);

        // Create the fade-in animation
        ObjectAnimator fadeIn = ObjectAnimator.ofFloat(imageView, "alpha", 0f, 1f);
        fadeIn.setDuration(1000); // Set the duration to 1 second (1000 milliseconds)

        // Start the animation
        fadeIn.start();
    }

    private void navigateToHome(){
        Intent intent = new Intent(WhoAreYou2.this, Home.class);
        startActivity(intent);
        //overrideActivityTransition(R.anim.slide_in_left);
        finish();
    }
}