package com.levislv.androiddemo.clipchildren;

import android.os.Bundle;
import android.util.Log;
import android.view.MotionEvent;
import android.view.View;

import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;

import com.levislv.androiddemo.R;

public class ClipChildrenActivity extends AppCompatActivity {

    private static final String TAG = "ClipChildrenActivity";

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_clipchildren);

        findViewById(R.id.view_content).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Log.d(TAG, "v: " + v);
            }
        });
    }
}