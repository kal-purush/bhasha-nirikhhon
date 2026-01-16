package com.example.duan1chinhthuc.Activity_user;

import android.content.Intent;
import android.os.Bundle;
import android.os.CountDownTimer;
import android.widget.Button;


import androidx.appcompat.app.AppCompatActivity;


import com.example.duan1chinhthuc.R;

public class Wellcome extends AppCompatActivity {


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_wellcome);

        Button btngetstarted = findViewById(R.id.btngetstarted);

        new CountDownTimer(3000,1000){

            @Override
            public void onTick(long l) {

            }

            @Override
            public void onFinish() {
                Intent intent = new Intent(Wellcome.this, dangnhap.class);
                startActivity(intent);


            }
        }.start();

    }
}