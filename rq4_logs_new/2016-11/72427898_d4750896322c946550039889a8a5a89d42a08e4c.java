package com.hormiga6.androidapipractice.ProgressBar;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;

import com.hormiga6.androidapipractice.R;

public class ProgressBarActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_progress_bar);

        //Progress bars out looking is material design style. Because theme is "@style/AppTheme".
        //You can't use theme like "@android:style/Theme.Translucent.NoTitleBar" because activity extends AppCompatActivity
        //Only center progress bar have theme before material design.
    }
}