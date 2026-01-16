package com.example.swu.quiz;

import android.content.Intent;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.widget.TextView;

public class endingActivity extends AppCompatActivity {


    private TextView t1, t2;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_ending);

        Intent i = getIntent();
        String message1 = i.getStringExtra("message1");
        String message2 = i.getStringExtra("m2");

        wireWidgets();


        t1.setText(message1);
        t2.setText(message2);


        }
    private void wireWidgets() {

        t1 = (TextView) findViewById(R.id.textView);
        t2 = (TextView) findViewById(R.id.text1);
    }
}