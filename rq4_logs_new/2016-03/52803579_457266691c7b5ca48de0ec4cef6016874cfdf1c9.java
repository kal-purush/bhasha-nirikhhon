package com.gropoid.jokelibrary;

import android.content.Context;
import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.widget.TextView;

public class JokeDisplayActivity extends AppCompatActivity {

    public static final String JOKE = "JOKE";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_joke_display);
        TextView jokeView = (TextView)findViewById(R.id.joke_view);
        jokeView.setText(getIntent().getStringExtra(JOKE));
    }

    public static void startWithJoke(Context context, String joke) {
        Intent intent = new Intent(context, JokeDisplayActivity.class);
        intent.putExtra(JOKE, joke);
        context.startActivity(intent);
    }
}