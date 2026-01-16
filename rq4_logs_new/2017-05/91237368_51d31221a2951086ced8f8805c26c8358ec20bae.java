package jp.gr.java_conf.ya.weartw; // Copyright (c) 2017 YA <ya.androidapp@gmail.com> All rights reserved. This software includes the work that is distributed in the Apache License 2.0

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.support.wearable.activity.ConfirmationActivity;
import android.support.wearable.view.DelayedConfirmationView;
import android.view.View;

import static jp.gr.java_conf.ya.weartw.TwitterUtil.webIntentUrlLike;

public class ActionActivity extends Activity implements
        DelayedConfirmationView.DelayedConfirmationListener {
    public static String webIntentUrlKey = "webIntentUrl";

    private DelayedConfirmationView mDelayedView;
    private String webIntentUrl;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_action);

        mDelayedView = (DelayedConfirmationView) findViewById(R.id.delayed_confirm);
        mDelayedView.setListener(this);

        // init
        webIntentUrl = "";
        // receive
        final Intent intent = getIntent();
        webIntentUrl = intent.getStringExtra(webIntentUrlKey);

        if(webIntentUrl.equals(""))
            finish();

        // wait
        mDelayedView.setTotalTimeMs(3000);
        mDelayedView.start();
    }

    @Override
    public void onTimerFinished(View view) {
        // not cancelled

        if(webIntentUrl.startsWith(webIntentUrlLike)){
            // favorite
            final String tweetIdStr = webIntentUrl.replace(webIntentUrlLike, "");
            final long tweetIdLong = Long.parseLong(tweetIdStr);
            final String statusStr = TwitterUtil.like(tweetIdLong);
            if(!statusStr.equals("")){
                // success
                final Intent intent = new Intent(this, ConfirmationActivity.class);
                intent.putExtra(ConfirmationActivity.EXTRA_ANIMATION_TYPE, ConfirmationActivity.SUCCESS_ANIMATION);
                intent.putExtra(ConfirmationActivity.EXTRA_MESSAGE, statusStr);
                startActivity(intent);
            }
        }
    }

    @Override
    public void onTimerSelected(View view) {
        finish(); // cancelled
    }
}