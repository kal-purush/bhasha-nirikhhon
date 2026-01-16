package com.anaoliveiravictoriamartins.animaladoption;

import android.graphics.Color;
import android.os.Bundle;

import androidx.appcompat.app.AppCompatActivity;

public class FormAnimal extends AppCompatActivity {
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.form_animal);

        initialCustomization();
    }

    private void initialCustomization(){
        if (getSupportActionBar() != null)
            getSupportActionBar().hide();

        getWindow().setStatusBarColor(Color.parseColor("#17706e"));
    }
}