package com.example.mojing;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.view.View;
import android.widget.ImageButton;
import android.widget.TextView;

public class Me_Figure_Activity extends AppCompatActivity {
    private TextView tv_shengao;
    private TextView tv_tizhong;
    private TextView tv_xiongwei;
    private TextView tv_yaowei;
    private TextView tv_tunwei;

    private SharedPreferencesManager sharedPreferencesManager;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_me_figure);

        //初始化
        tv_shengao= findViewById(R.id.tv_shengao);
        tv_tizhong = findViewById(R.id.tv_tizhong);
        tv_yaowei =findViewById(R.id.tv_yaowei);
        tv_xiongwei =findViewById(R.id.tv_xiongwei);
        tv_yaowei = findViewById(R.id.tv_yaowei);
        tv_tunwei=findViewById(R.id.tv_tunwei);
        sharedPreferencesManager = new SharedPreferencesManager(this);

        tv_shengao.setText(sharedPreferencesManager.getFigureShengao());
        tv_tizhong.setText(sharedPreferencesManager.getFigureTizhong());
        tv_tunwei.setText(sharedPreferencesManager.getFigureTunwei());
        tv_xiongwei.setText(sharedPreferencesManager.getFigureXiongwei());
        tv_yaowei.setText(sharedPreferencesManager.getFigureYaowei());

        //返回按钮
        ImageButton btnBack = findViewById(R.id.btn_back);
        btnBack.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                finish(); // 返回上一个Activity
            }
        });
    }
}