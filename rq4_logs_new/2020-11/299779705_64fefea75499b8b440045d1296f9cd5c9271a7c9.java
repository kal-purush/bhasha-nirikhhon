package com.example.cmput301f20t18;

import android.graphics.Bitmap;
import android.os.Bundle;
import android.widget.Button;

import androidx.annotation.Nullable;
import androidx.fragment.app.FragmentActivity;
import androidx.viewpager.widget.ViewPager;

import java.util.ArrayList;

public class imageSliderActivity extends FragmentActivity {


    ViewPager mPager;
    ImagePageAdapter imageAdapter;
    Button sliderButton;

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.fragment_image_slider);
        ArrayList<Bitmap> photos = (ArrayList<Bitmap>) getIntent().getExtras().get("Photos");
        mPager = findViewById(R.id.pag);

        imageAdapter = new ImagePageAdapter(getSupportFragmentManager(), 1, photos);

    }
}