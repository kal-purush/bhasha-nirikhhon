package org.sochidrive.weather;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

import androidx.appcompat.app.AppCompatActivity;

public class SettingsActivity extends AppCompatActivity {
    Button buttonBack;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_settings);
        initView();
        setOnClickButton();
    }

    private void initView() {
        buttonBack = findViewById(R.id.buttonBack);
    }

    private void setOnClickButton() {
        buttonBack.setOnClickListener(onClickListenerMain);
    }

    private View.OnClickListener onClickListenerMain = new View.OnClickListener() {
        @Override
        public void onClick(View view) {
            Intent intent = new Intent(SettingsActivity.this,MainActivity.class);
            startActivity(intent);
        }
    };
}