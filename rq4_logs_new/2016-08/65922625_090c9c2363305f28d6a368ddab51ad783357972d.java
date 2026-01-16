package com.edwincobos.subscribersapp.main;

import android.content.Intent;
import android.os.Handler;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;

import com.edwincobos.subscribersapp.R;
import com.edwincobos.subscribersapp.subscriberslist.SubscribersListActivity;

import pl.droidsonroids.gif.GifImageView;

public class MainActivity extends AppCompatActivity {

    private GifImageView gifImageView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        getSupportActionBar().hide();
        startAnimation();
        startTimer();
    }

    private void startAnimation() {
        gifImageView = (GifImageView) findViewById(R.id.splashLogo);

        gifImageView.setScaleX(0.0f);
        gifImageView.setScaleY(0.0f);
        gifImageView.animate().setStartDelay(200).setDuration(1300).scaleX(1.0f).scaleY(1.0f);
    }

    private void startTimer(){
        new Handler().postDelayed(new Runnable() {
            @Override
            public void run() {
                Intent intent = new Intent();
                intent.setClass(getApplicationContext(), SubscribersListActivity.class);
                startActivity(intent);
                //overridePendingTransition(R.anim.effect_fade_in, R.anim.effect_fade_out);
                finish();
            }
        }, 3000);

    }
}