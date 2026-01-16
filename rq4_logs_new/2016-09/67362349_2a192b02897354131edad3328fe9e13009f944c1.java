package com.repofetcher;

import android.content.Intent;
import android.os.Handler;
import android.support.v7.app.AppCompatActivity;

/**
 * Created by ricar on 29/09/2016.
 */

public class SplashScreenActivity extends AppCompatActivity{

    @Override
    protected void onResume() {
        super.onResume();
        new Handler().postDelayed(new Runnable() {
            @Override
            public void run() {
                Intent intent = new Intent(SplashScreenActivity.this, MainActivity.class);
                startActivity(intent);
                finish();
            }
        }, 5000);

    }
}