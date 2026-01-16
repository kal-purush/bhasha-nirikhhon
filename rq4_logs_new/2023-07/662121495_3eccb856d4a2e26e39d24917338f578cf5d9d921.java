package com.example.mojing;

import android.content.Intent;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.ImageButton;

import androidx.appcompat.app.AppCompatActivity;

/**
 * Checkout implementation for the app
 */
public class VipChatActivity extends AppCompatActivity {
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_vip_chat);

        ImageButton btnBack = findViewById(R.id.back_button);
        btnBack.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                finish(); // 返回上一个Activity
            }
        });
//        Button btn_personal = findViewById(R.id.btn_personal);
//        btn_personal.setOnClickListener(new View.OnClickListener() {
//            @Override
//            public void onClick(View v) {
//                Intent intent = new Intent(VipChatActivity.this,Dapei_Album_Activity.class);
//                //传递额外的数据到目标Activity
//                intent.putExtra("key", "个人搭配");
//                startActivity(intent);
//            }
//        });
//        Button btn_ai = findViewById(R.id.btn_ai);
//        btn_ai.setOnClickListener(new View.OnClickListener() {
//            @Override
//            public void onClick(View v) {
//                Intent intent = new Intent(VipChatActivity.this,Dapei_Album_Activity.class);
//                //传递额外的数据到目标Activity
//                intent.putExtra("key", "AI");
//                startActivity(intent);
//            }
//        });
//        Button btn_designer = findViewById(R.id.btn_designer);
//        btn_designer.setOnClickListener(new View.OnClickListener() {
//            @Override
//            public void onClick(View v) {
//                Intent intent = new Intent(VipChatActivity.this,Dapei_Info_Activity.class);
//                //传递额外的数据到目标Activity
//                intent.putExtra("key", "设计师");
//                startActivity(intent);
//            }
//        });
    }
}