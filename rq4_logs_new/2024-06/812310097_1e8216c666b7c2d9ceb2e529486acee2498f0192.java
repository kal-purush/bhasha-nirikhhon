package com.example.project58;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

import androidx.appcompat.app.AppCompatActivity;

public class GenderActivity extends AppCompatActivity {

    private Button buttonMale;
    private Button buttonFemale;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_gender);

        buttonMale = findViewById(R.id.buttonMale);
        buttonFemale = findViewById(R.id.buttonFemale);

        // 성별 선택 버튼 클릭 이벤트 처리
        buttonMale.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // 남성 버튼 클릭 시 YearActivity로 이동
                Intent intent = new Intent(GenderActivity.this, YearActivity.class);
                startActivity(intent);
            }
        });

        buttonFemale.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // 여성 버튼 클릭 시 YearActivity로 이동
                Intent intent = new Intent(GenderActivity.this, YearActivity.class);
                startActivity(intent);
            }
        });
    }
}