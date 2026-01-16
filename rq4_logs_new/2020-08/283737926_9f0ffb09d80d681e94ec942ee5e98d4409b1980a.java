package com.example.geekbrainsandroidweather.custom_views;

import android.animation.ObjectAnimator;
import android.animation.ValueAnimator;
import android.content.Context;
import android.content.Intent;
import android.graphics.Canvas;
import android.net.Uri;
import android.util.AttributeSet;
import android.view.LayoutInflater;
import android.view.View;
import android.view.animation.Animation;
import android.view.animation.AnimationUtils;
import android.view.animation.LinearInterpolator;
import android.view.animation.RotateAnimation;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.constraintlayout.widget.ConstraintLayout;

import com.example.geekbrainsandroidweather.R;

public class HumidityView extends ConstraintLayout {
    private TextView humidityTextView;
    private TextView sunriseAndSunsetTextView;
    private ImageView humidityImageView;

    public HumidityView(Context context) {
        super(context);
        initViews(context);
    }

    public HumidityView(Context context, AttributeSet attrs) {
        super(context, attrs);
        initViews(context);
    }

    public HumidityView(Context context, AttributeSet attrs, int defStyleAttr) {
        super(context, attrs, defStyleAttr);
        initViews(context);
    }

    private void initViews(Context context) {
        LayoutInflater inflater = (LayoutInflater) context
                .getSystemService(Context.LAYOUT_INFLATER_SERVICE);
        inflater.inflate(R.layout.humidity_view, this);
        initUI();
        setupImageViewAnimation();
    }

    private void initUI() {
        humidityTextView = this.findViewById(R.id.humidityTextView);
        sunriseAndSunsetTextView = this.findViewById(R.id.sunriseAndSunsetTextView);
        humidityImageView = this.findViewById(R.id.humidityImageView);
    }

    public void setupTexts(String humidity, String sunriseAndSunset) {
        humidityTextView.setText(humidity);
        sunriseAndSunsetTextView.setText(sunriseAndSunset);
        invalidate();
    }

    private void setupImageViewAnimation() {
        ObjectAnimator objectAnimator = ObjectAnimator.ofFloat(humidityImageView, View.ALPHA, 0, 1).setDuration(4000);
        objectAnimator.setRepeatCount(ValueAnimator.INFINITE);
        objectAnimator.start();

        ObjectAnimator objectAnimator1 = ObjectAnimator.ofFloat(humidityImageView, View.TRANSLATION_Y, 20).setDuration(4000);
        objectAnimator1.setRepeatCount(ValueAnimator.INFINITE);
        objectAnimator1.start();
    }

}