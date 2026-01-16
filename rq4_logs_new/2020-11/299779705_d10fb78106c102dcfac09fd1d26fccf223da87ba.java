package com.example.cmput301f20t18;

import androidx.appcompat.app.AppCompatActivity;
import androidx.viewpager.widget.ViewPager;

import android.os.Bundle;
import android.view.View;
import android.widget.Button;

public class ImageSliderActivity extends AppCompatActivity {

    ViewPager mPager;
    ImagePageAdapter imageAdapter;
    Button sliderButton;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_image_slider);

        sliderButton = findViewById(R.id.slider_return_button);

        /*mPager = findViewById(R.id.slider_viewer);
        imageAdapter = new ImagePageAdapter (getSupportFragmentManager(),1);
        mPager.setAdapter(imageAdapter);*/

        sliderButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {



            }
        });

    }
}