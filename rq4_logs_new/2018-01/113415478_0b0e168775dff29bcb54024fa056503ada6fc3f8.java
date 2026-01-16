package com.example.cz10000_001.mytestapp;

import android.content.Context;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;

import com.example.cz10000_001.mytestapp.wallpager.VideoWallPager;

public class MyWallPageActivity extends AppCompatActivity {

    private Context context  = null;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_my_wall_page);

        context = this ;

        findViewById(R.id.videobtn).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                VideoWallPager.setToWallPaper(context);
            }
        });
    }


}