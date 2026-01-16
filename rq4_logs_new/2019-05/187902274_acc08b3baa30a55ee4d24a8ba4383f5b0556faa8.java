package com.example.healthtracker;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.widget.ImageView;
import android.widget.Toast;

import com.synnapps.carouselview.CarouselView;
import com.synnapps.carouselview.ImageClickListener;
import com.synnapps.carouselview.ImageListener;

public class carouselActivity extends AppCompatActivity {
    private int[] mImages = new int[]{
            R.drawable.image_bball,
            R.drawable.image_dumbbells,
            R.drawable.image_kettle,
            R.drawable.image_rope,
    };
    private String[] mImageTitle = new String[]{
            "A new basket ball on a vibrant wood floor",
            "A woman reaching for a dumbbell from the rack",
            "A woman holding one of those kettle things",
            "Some thick and foreboding looking rope"
    };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_carousel);

        CarouselView carouselView = findViewById(R.id.carousel);
        carouselView.setPageCount(mImages.length);
        carouselView.setImageListener(new ImageListener() {
            @Override
            public void setImageForPosition(int position, ImageView imageView) {
                imageView.setImageResource(mImages[position]);
            }
        });
        carouselView.setImageClickListener(new ImageClickListener() {
            @Override
            public void onClick(int position) {
                Toast.makeText(carouselActivity.this, mImageTitle[position], Toast.LENGTH_SHORT).show();
            }
        });
    }
}