package com.example.hllogeegbrains;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

import androidx.appcompat.app.AppCompatActivity;

public class CalendarActivity extends AppCompatActivity implements View.OnClickListener {
    private Button switchButton;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_calendar);

        switchButton = findViewById(R.id.calendarGoBackButton);
        switchButton.setOnClickListener(this);
    }

    @Override
    public void onClick(View view) {
        if (view.getId() == R.id.calendarGoBackButton) {
            Intent intent = new Intent(this, SideActivity.class);
            startActivity(intent);
        }
    }
}