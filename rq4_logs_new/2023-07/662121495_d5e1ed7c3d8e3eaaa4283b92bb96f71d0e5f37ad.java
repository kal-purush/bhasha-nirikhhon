package com.example.mojing;

import androidx.appcompat.app.AppCompatActivity;

import android.annotation.SuppressLint;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.SeekBar;
import android.widget.TextView;

public class InitFigureActivity extends AppCompatActivity {
    private SharedPreferencesManager sharedPreferencesManager;
    private SeekBar shengaoseekbar;
    private SeekBar tizhongseekbar;
    private TextView skipBtn;
    private TextView shengaotext;
    private TextView tizhongtext;
    private Button nextBtn;
    private int shengao=160;
    private  int tizhong = 50;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_init_figure);
        shengaotext=findViewById(R.id.shengaotext);
        shengaoseekbar = findViewById(R.id.shengaoseekBar);
        tizhongtext= findViewById(R.id.tizhongtext);
        tizhongseekbar = findViewById(R.id.tizhongseekBar);
        nextBtn =findViewById(R.id.nextBtn);
        skipBtn = findViewById(R.id.skip);
        sharedPreferencesManager = new SharedPreferencesManager(this);

        skipBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                sharedPreferencesManager.setIsYouke(true);
                Intent tmp = new Intent(InitFigureActivity.this, MainActivity.class);
                startActivity(tmp);
                finish();
            }
        });
        nextBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                sharedPreferencesManager.setFigureShengao(Integer.toString(shengao));
                sharedPreferencesManager.setFigureTizhong(Integer.toString(tizhong));

                Intent tmp = new Intent(InitFigureActivity.this, InitSanweiActivity.class);
                startActivity(tmp);
                finish();
            }
        });
        shengaoseekbar.setOnSeekBarChangeListener(new SeekBar.OnSeekBarChangeListener() {
            @SuppressLint("SetTextI18n")
            @Override
            public void onProgressChanged(SeekBar seekBar, int progress, boolean fromUser) {
                // 当SeekBar的进度发生改变时调用，fromUser表示是否是用户拖动改变的进度
                // 这里可以获取当前进度值
                int currentProgress = progress;
                shengaotext.setText(currentProgress+"cm");
                shengao=currentProgress;
                // 进行相关操作，比如更新UI显示当前值
            }

            @Override
            public void onStartTrackingTouch(SeekBar seekBar) {
                // 当用户开始拖动SeekBar时调用
            }

            @Override
            public void onStopTrackingTouch(SeekBar seekBar) {
                // 当用户停止拖动SeekBar时调用
            }
        });
        tizhongseekbar.setOnSeekBarChangeListener(new SeekBar.OnSeekBarChangeListener() {
            @SuppressLint("SetTextI18n")
            @Override
            public void onProgressChanged(SeekBar seekBar, int progress, boolean fromUser) {
                // 当SeekBar的进度发生改变时调用，fromUser表示是否是用户拖动改变的进度
                // 这里可以获取当前进度值
                int currentProgress = progress;
                tizhongtext.setText(currentProgress+"kg");
                tizhong=currentProgress;
                // 进行相关操作，比如更新UI显示当前值
            }

            @Override
            public void onStartTrackingTouch(SeekBar seekBar) {
                // 当用户开始拖动SeekBar时调用
            }

            @Override
            public void onStopTrackingTouch(SeekBar seekBar) {
                // 当用户停止拖动SeekBar时调用
            }
        });
    }
}