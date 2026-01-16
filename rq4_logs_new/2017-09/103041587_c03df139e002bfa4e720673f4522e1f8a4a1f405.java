package com.example.bingo.helloworld;

import android.support.v4.util.LogWriter;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;

public class HelloWorldActivity extends AppCompatActivity {
    
    private static final String TAG = "HelloWorldActivity";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_hello_world);
        Log.d(TAG, "onCreate: execute");
        Log.d(TAG, "onCreate: execute12");
        Log.d("execute", "onCreate: execute12");
    }
}