package com.example.leccion_amst;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;

public class DetalleHero extends AppCompatActivity {
    private String id_hero;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_detalle_hero);

        Intent intent = getIntent();
        id_hero = intent.getStringExtra("id_hero");
        System.out.println(id_hero);

    }
}