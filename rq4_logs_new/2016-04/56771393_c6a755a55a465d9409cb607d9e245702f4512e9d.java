package com.example.mridul_xpetize.test;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.ImageView;

import com.squareup.picasso.Picasso;

public class CompletedTaskActivity extends AppCompatActivity {

    ImageView img;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_completed_task);

        img = (ImageView)findViewById(R.id.imageView2);
        final String image_url = "https://www.google.co.in/images/branding/googleg/1x/googleg_standard_color_128dp.png";
        Picasso.with(CompletedTaskActivity.this)
                .load(image_url)
//                .placeholder(R.drawable.no_image)
                .into(img);

        img.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                Intent i = new Intent(CompletedTaskActivity.this, FullImageActivity.class);
                i.putExtra("type", "url");
                i.putExtra("image", image_url);
                startActivity(i);
            }
        });
    }
}