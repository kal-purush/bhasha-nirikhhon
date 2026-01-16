package com.example.geekbrainsandroidweather;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;

public class DevelopersInfoActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        if (SettingsActivity.isDarkTheme) {
            setTheme(R.style.DarkTheme);
        } else {
            setTheme(R.style.AppTheme);
        }
        setContentView(R.layout.activity_developers_info);
    }

}