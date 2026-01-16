package com.example.onsen.onsenapplication;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.widget.TextView;

public class Main2Activity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main2);
        Intent intent = getIntent();
        String name = intent.getStringExtra("editTextName");
        String surname = intent.getStringExtra("editTextLName");
        String age = intent.getStringExtra("editTextAge");
        String add = intent.getStringExtra("editTextAddress");
        TextView textName = (TextView) findViewById(R.id.textView9);
        TextView textsurName = (TextView) findViewById(R.id.textView10);
        TextView textage = (TextView) findViewById(R.id.textView11);
        TextView textadd = (TextView) findViewById(R.id.textView12);
        textName.setText(name);
        textsurName.setText(surname);
        textage.setText(age);
        textadd.setText(add);
    }
}