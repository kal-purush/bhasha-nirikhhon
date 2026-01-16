package com.example.artisanprofilingapp;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

public class ImageCaptureSelection extends AppCompatActivity {
    Button saree, kurtya, tshirt, showpiece, bag, goina;
    private SharedPreferences myPref;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_image_capture_selection);
        saree = findViewById(R.id.saree);
        kurtya = findViewById(R.id.kurta);
        tshirt = findViewById(R.id.tshirt);
        showpiece = findViewById(R.id.showPiece);
        bag = findViewById(R.id.bag);
        goina = findViewById(R.id.goina);
        myPref = getApplicationContext().getSharedPreferences("MyPref",MODE_PRIVATE);
        saree.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                myPref.edit().putString("selected","saree").apply();
                startActivity(new Intent(getApplicationContext(),Insert_image_instructionActivity.class));
            }
        });
        kurtya.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                myPref.edit().putString("selected","kurta").apply();
                startActivity(new Intent(getApplicationContext(),Insert_image_instructionActivity.class));
            }
        });
        tshirt.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                myPref.edit().putString("selected","tshirt").apply();
                startActivity(new Intent(getApplicationContext(),Insert_image_instructionActivity.class));
            }
        });
        showpiece.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                myPref.edit().putString("selected","showpiece").apply();
                startActivity(new Intent(getApplicationContext(),Insert_image_instructionActivity.class));
            }
        });
        bag.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                myPref.edit().putString("selected","bag").apply();
                startActivity(new Intent(getApplicationContext(),Insert_image_instructionActivity.class));
            }
        });
        goina.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                myPref.edit().putString("selected","goina").apply();
                startActivity(new Intent(getApplicationContext(),Insert_image_instructionActivity.class));
            }
        });

    }


}