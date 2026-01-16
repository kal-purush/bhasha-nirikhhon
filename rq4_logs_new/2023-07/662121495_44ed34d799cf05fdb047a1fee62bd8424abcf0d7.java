package com.example.mojing;

import androidx.appcompat.app.AppCompatActivity;

import android.annotation.SuppressLint;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.GridLayout;
import android.widget.ImageButton;
import android.widget.TextView;

public class Dapei_Album_Activity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_dapei_album);

        Intent intent = getIntent();
        //获取传递的数据值
        String message = intent.getStringExtra("key");

        TextView text = findViewById(R.id.title);
        text.setText("搭配相册" + message);

        //返回按钮
        ImageButton btnBack = findViewById(R.id.btn_back);
        btnBack.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                finish(); // 返回上一个Activity
            }
        });
    }

    protected void Load_Picture() {
        GridLayout gridLayout = findViewById(R.id.grid_layout);
//        for (int i = 0; i < numberOfImages; i++) {
//            ImageView imageView = new ImageView(this);
//            // 设置图片资源或其他属性
//            imageView.setImageResource(R.drawable.image_resource);
//            // 可以为图片设置宽高等布局参数
//            GridLayout.LayoutParams params = new GridLayout.LayoutParams();
//            params.width = GridLayout.LayoutParams.WRAP_CONTENT;
//            params.height = GridLayout.LayoutParams.WRAP_CONTENT;
//            // 设置图片在GridLayout中的位置
//            params.rowSpec = GridLayout.spec(i / numberOfColumns);
//            params.columnSpec = GridLayout.spec(i % numberOfColumns);
//            // 添加图片视图到GridLayout
//            gridLayout.addView(imageView, params);
//        }
    }
}