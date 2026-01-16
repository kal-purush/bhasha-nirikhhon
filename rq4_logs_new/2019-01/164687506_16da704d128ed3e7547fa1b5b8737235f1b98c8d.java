package com.bassett.health_tracker_app;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.TextView;

public class FingerExercise extends AppCompatActivity {

    public Integer currentCount = 0;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_finger_exercise);

        // Get the Intent that started this activity and extract the string
        Intent intent = getIntent();
        String message = intent.getStringExtra(MainActivity.EXTRA_MESSAGE);

        // Capture the layout's TextView and set the string as its text
        TextView textView = findViewById(R.id.textView3);
        textView.setText(message);
    }

    public void onButtonClick (View v){
        System.out.println("button was clicked");
        TextView textView = findViewById(R.id.textView);
        currentCount = currentCount + 1;
        String everyTenEncouragement = " good job keep it up ";
        String everyFiftyEncouragement = " omg you are amazing";
        if (currentCount % 10 == 0 && currentCount % 50 != 0) {
            String currentCountString = currentCount.toString();
            textView.setText(currentCountString + everyTenEncouragement);
        } else if (currentCount % 50 == 0) {
            String currentCountString = currentCount.toString();
            textView.setText(currentCountString + everyFiftyEncouragement);
        } else {
            String currentCountString = currentCount.toString();
            textView.setText(currentCountString);
        }

    }
}