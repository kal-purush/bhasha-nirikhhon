package com.example.enigmator.activity;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;

import com.example.enigmator.R;
import com.example.enigmator.fragment.SettingsFragment;

public class SettingsActivity extends AppCompatActivity {
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_settings);
        getSupportFragmentManager()
                .beginTransaction()
                .replace(R.id.settings_container, new SettingsFragment())
                .commit();
    }
}