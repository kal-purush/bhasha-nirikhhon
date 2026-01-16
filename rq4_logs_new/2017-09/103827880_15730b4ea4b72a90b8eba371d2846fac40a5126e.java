package com.example.jiaminwang.test_nearby;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

public class secondaryActivity extends AppCompatActivity {
    private Button primaryScreenButton;
    private Button tertiaryScreenButton;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_secondary);
        primaryScreenButton = (Button) findViewById(R.id.secondaryButton1);
        tertiaryScreenButton = (Button) findViewById(R.id.secondaryButton2);
        primaryScreenButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(secondaryActivity.this, MapsActivity.class);
                startActivity(intent);
            }
        });
        tertiaryScreenButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(secondaryActivity.this, tertiaryActivity.class);
                startActivity(intent);
            }
        });


    }
}