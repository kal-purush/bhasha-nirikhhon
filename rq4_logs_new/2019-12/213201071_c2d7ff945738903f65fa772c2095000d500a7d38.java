package com.example.chores.ativities;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;

import com.example.chores.MainActivity;
import com.example.chores.R;
import com.example.chores.ativities.ui.login.LoginActivity;

public class MainEmptyActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        boolean loggedIn = false;

        Intent activityIntent;

        if (loggedIn) {
            activityIntent = new Intent(this, MainActivity.class);
        } else {
            activityIntent = new Intent(this, LoginActivity.class);
        }

        startActivity(activityIntent);
        setContentView(R.layout.activity_main_empty);
    }
}