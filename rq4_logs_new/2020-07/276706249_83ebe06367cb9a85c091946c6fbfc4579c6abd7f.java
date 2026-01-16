package com.dtavana.foodswipe.activities;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;

import com.dtavana.foodswipe.databinding.ActivityMainBinding;

public class MainActivity extends AppCompatActivity {

    ActivityMainBinding binding;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        binding = ActivityMainBinding.inflate(getLayoutInflater());
        setContentView(binding.getRoot());
    }
}