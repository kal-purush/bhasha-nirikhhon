package com.arbaini.getih;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

public class JadiDonorActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_jadi_donor);
        getSupportActionBar().setTitle("Jadi Pendonor");
        Button donorkan = findViewById(R.id.cv_donorkan);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);


        donorkan.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                startActivity(new Intent(JadiDonorActivity.this, MainActivity.class));
            }
        });
    }
}