package com.softwareoverflow.colorfall.activities;

import android.app.Activity;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.os.Handler;
import android.support.v7.app.AppCompatActivity;
import android.text.method.LinkMovementMethod;
import android.util.Log;
import android.view.View;
import android.view.animation.Animation;
import android.widget.TextView;

import com.google.firebase.analytics.FirebaseAnalytics;
import com.softwareoverflow.colorfall.R;
import com.softwareoverflow.colorfall.animations.FadeInOutAnimation;
import com.softwareoverflow.colorfall.media.BackgroundMusicService;

public class ConsentActivity extends AppCompatActivity implements View.OnClickListener{

    public enum Consent { GIVEN, NOT_GIVEN, UNKNOWN }

    public static Consent userConsent = Consent.UNKNOWN;

    private TextView giveConsent, revokeConsent;
    private Consent originalConsent;
    private boolean isAnimatingViews = false;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_consent);

        originalConsent = userConsent;

        TextView consentInfo = findViewById(R.id.consent_info_tv);
        consentInfo.setMovementMethod(LinkMovementMethod.getInstance());

        giveConsent = findViewById(R.id.consent_to_analytics);
        giveConsent.setOnClickListener(this);
        revokeConsent = findViewById(R.id.revoke_consent_to_analytics);
        revokeConsent.setOnClickListener(this);
    }

    @Override
    public void onClick(View v) {
        switch (v.getId()){
            case R.id.consent_to_analytics:
                userConsent = Consent.GIVEN;
                sendAnalytics();
                break;
            case R.id.revoke_consent_to_analytics:
                userConsent = Consent.NOT_GIVEN;
                break;
        }

        SharedPreferences sp = getSharedPreferences("settings", MODE_PRIVATE);
        sp.edit().putString("consent", userConsent.name()).apply();

        Intent returnIntent = new Intent();
        setResult(Activity.RESULT_OK, returnIntent);
        BackgroundMusicService.changingActivity = true;
        finish();
    }

    private void sendAnalytics(){
        FirebaseAnalytics analytics = FirebaseAnalytics.getInstance(this);
        Bundle eventInfo = new Bundle();
        eventInfo.putString("from_consent", originalConsent.name());
        analytics.logEvent("gave_consent", eventInfo);
    }

    @Override
    public void onBackPressed() {
        //don't animate if already animating
        if(isAnimatingViews){
            return;
        }

        //allow the user to back out if they've already set their preference
        if(userConsent != Consent.UNKNOWN){
            Intent returnIntent = new Intent();
            setResult(Activity.RESULT_CANCELED, returnIntent);
            BackgroundMusicService.changingActivity = true;
            finish();
        }

        isAnimatingViews = true;
        //use a new FadeInOutAnimation to stop them going in time (slightly hacky)
        final Animation fadeInOut = new FadeInOutAnimation(3);
        fadeInOut.setAnimationListener(new Animation.AnimationListener() {
            @Override
            public void onAnimationStart(Animation animation) { /*do nothing */ }

            @Override
            public void onAnimationEnd(Animation animation) { isAnimatingViews = false; }

            @Override
            public void onAnimationRepeat(Animation animation) { /* do nothing */ }
        });

        long duration = fadeInOut.getDuration();

        giveConsent.startAnimation(new FadeInOutAnimation(3));
        new Handler().postDelayed(new Runnable(){
            @Override
            public void run() {
                revokeConsent.startAnimation(fadeInOut);
            }
        }, duration / 2);
    }

    @Override
    protected void onResume() {
        Log.d("debug", "Consent: " + BackgroundMusicService.changingActivity);

        if(!BackgroundMusicService.changingActivity) {
            startService(new Intent(this, BackgroundMusicService.class));
        }
        BackgroundMusicService.changingActivity = false;

        super.onResume();
    }

    @Override
    protected void onPause() {
        if(!BackgroundMusicService.changingActivity) {
            stopService(new Intent(this, BackgroundMusicService.class));
        }
        super.onPause();
    }
}