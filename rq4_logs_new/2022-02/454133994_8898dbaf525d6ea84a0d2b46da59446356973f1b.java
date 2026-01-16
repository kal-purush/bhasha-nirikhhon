package com.codewithArdents.dysgraphia;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.widget.RadioGroup;

public class gender extends AppCompatActivity {
    private RadioGroup radiogrp;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_gender);

        RadioGroup radioGroup = (RadioGroup) findViewById(R.id.radiogrp);
        radioGroup.setOnCheckedChangeListener(new RadioGroup.OnCheckedChangeListener()
        {
            @Override
            public void onCheckedChanged(RadioGroup group, int checkedId) {
                // checkedId is the RadioButton selected
                switch (checkedId){
                    case R.id.custom_button1:
                        findViewById(R.id.custom_button1).setBackground(getResources().getDrawable(R.drawable.r_boy));
                        findViewById(R.id.custom_button2).setBackground(getResources().getDrawable(R.drawable.girl));

                        break;
                    case R.id.custom_button2:
                        findViewById(R.id.custom_button2).setBackground(getResources().getDrawable(R.drawable.r_girl));
                        findViewById(R.id.custom_button1).setBackground(getResources().getDrawable(R.drawable.boy));
                        break;
                }


            }
        });

    }
}