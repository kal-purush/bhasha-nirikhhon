package com.epam.traing.gitcl.ui;

import android.content.Intent;
import android.os.Handler;
import android.support.v4.app.ActivityOptionsCompat;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.ImageView;

import com.epam.traing.gitcl.R;
import com.epam.traing.gitcl.helper.PrefHelper;

import butterknife.Bind;
import butterknife.ButterKnife;

public class CurtainActivity extends AppCompatActivity {

    @Bind(R.id.imgLogo)
    ImageView imgLogo;

    boolean isNextScreenStarted = false;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_curtain);
        ButterKnife.bind(this);
    }

    @Override
    protected void onResume() {
        super.onResume();

        if (!isNextScreenStarted) {
            goToNextScreen();
            isNextScreenStarted = true;
        }
    }

    private void goToNextScreen() {
        new Handler().postDelayed(new Runnable() {
            @Override
            public void run() {
                if (PrefHelper.showLoginScreen(CurtainActivity.this)) {
                    startLoginScreen();
                } else {
                    startMainScreen();
                }
            }
        }, 500);
    }

    private void startLoginScreen() {
        ActivityOptionsCompat options = ActivityOptionsCompat.
                makeSceneTransitionAnimation(this, imgLogo, this.getString(R.string.transition_logo));
        startActivity(new Intent(this, LoginActivity.class), options.toBundle());
    }

    private void startMainScreen() {
        startActivity(new Intent(this, MainActivity.class));
        overridePendingTransition(android.R.anim.fade_in, android.R.anim.fade_out);
    }

}