package com.example.fashop.activity;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.view.View;
import android.widget.TextView;

import com.example.fashop.R;

public class AboutUsActivity extends AppCompatActivity {
    private TextView tvTitle1;
    private TextView tvTitle2;
    private TextView tvContent1;
    private TextView tvContent2;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_about_us);

        initUI();
    }

    private void initUI() {
        tvTitle1 = findViewById(R.id.tvFirstHelpTitle);
        tvTitle2 = findViewById(R.id.tvSecondHelpTitle);
        tvContent1 = findViewById(R.id.tvFirstHelpContent);
        tvContent2 = findViewById(R.id.tvSecondHelpContent);

        tvTitle1.setOnClickListener(view -> {
            if (tvContent1.getVisibility() == View.GONE)
            {
                tvContent1.setVisibility(View.VISIBLE);
            }
            else
            {
                tvContent1.setVisibility(View.GONE);
            }
        });

        tvTitle2.setOnClickListener(view -> {
            if (tvContent2.getVisibility() == View.GONE)
            {
                tvContent2.setVisibility(View.VISIBLE);
            }
            else
            {
                tvContent2.setVisibility(View.GONE);
            }
        });
    }
}