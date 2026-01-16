package com.example.penguinone;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.TextView;

import java.util.Locale;

public class QuizAfterActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_quiz_after);
        int score = getIntent().getExtras().getInt("score");
        TextView scoreElement = (TextView) findViewById(R.id.score);
        scoreElement.setText(String.format(Locale.US, "%d%%", score));
    }

    public void home(View view) {
        Intent i = new Intent(this, TitleActivity.class);
        startActivity(i);
    }
}