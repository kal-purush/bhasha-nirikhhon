package com.example.android.whatevertrash;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.widget.TextView;

public class descriptionpage extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.descriptionpage);
        final TextView title = findViewById(R.id.descriptiontitle);
        final TextView description = findViewById(R.id.descriptiondescription);
        title.setText(getIntent().getStringExtra("locationtitle"));
        description.setText(getIntent().getStringExtra("locationdescription"));
    }

}