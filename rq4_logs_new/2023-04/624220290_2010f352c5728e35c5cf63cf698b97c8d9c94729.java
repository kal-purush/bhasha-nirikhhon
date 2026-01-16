package com.example.fashop.activity;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.view.View;
import android.widget.TextView;

import com.example.fashop.R;

public class CommunityRulesActivity extends AppCompatActivity {
    private TextView tvTitle1;
    private TextView tvTitle2;
    private TextView tvContent1;
    private TextView tvContent2;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_community_rules);

        initUI();
    }

    private void initUI() {
        tvTitle1 = findViewById(R.id.tvFirstTitle);
        tvTitle2 = findViewById(R.id.tvSecondTitle);
        tvContent1 = findViewById(R.id.tvFirstContent);
        tvContent2 = findViewById(R.id.tvSecondContent);

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