package com.client.client;

import android.content.Intent;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

/**
 * Created by Administrator on 2015/12/31.
 */
public class ForgetActivity extends AppCompatActivity {
    private TextView alreadyuser;
    private Button text_message;

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.forget);
        alreadyuser = (TextView) findViewById(R.id.alreadyuser);
        text_message = (Button) findViewById(R.id.text_message);
        alreadyuser.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(ForgetActivity.this, SignInActivity.class);
                startActivity(intent);
            }
        });
        text_message.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(ForgetActivity.this,ResetActivity.class);
                startActivity(intent);
            }
        });
    }
}