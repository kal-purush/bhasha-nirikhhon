package com.example.pacmanlike.activities;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.TextView;

import com.example.pacmanlike.R;

public class GameOverActivity extends AppCompatActivity {
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_game_over);

        Intent intent = getIntent();
        /*
        // Insert this into GameScreen at the beginning:
        public static final String SCORE = "com.example.pacmanlike.SCORE";

        // And when opening this activity:
        // intent takes context, it should be GameScreen activity -> pass it to somewhere you're calling this from
        // and replace GameScreen.this with it
        Intent intent = new Intent(GameScreen.this, GameOverActivity.class);
        intent.putExtra(SCORE, stringExtra);

        startActivity(intent);
         */

        //   Then uncomment this:
        // String stringScore = intent.getStringExtra(GameScreen.SCORE);
        // int score = Integer.parseInt(stringScore);

        // TextView scoreView = (TextView) findViewById(R.id.score_box);
        // scoreView.setText(R.string.score_text + score);
    }

    public void onRetryClick(View view){
        super.finish();
    }
}