package com.example.brian.hackucsc;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;

/**
 * Created by Brian on 1/21/17.
 */

public class Activity2 extends MainActivity {
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.test);

    }
    public void goback(View view){
        Intent intent = new Intent(this, MainActivity.class);
        startActivity(intent);
    }
}