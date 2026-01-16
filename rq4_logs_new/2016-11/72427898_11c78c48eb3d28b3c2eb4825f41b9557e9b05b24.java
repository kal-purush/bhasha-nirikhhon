package com.hormiga6.androidapipractice.ActivityResult;

import android.content.Intent;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.view.View;

import com.hormiga6.androidapipractice.R;

public class ResultActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_result);
    }

    public void justFinish(View view){
        finish();
    }

    public void finishWithResultCode(View view){
        setResult(RESULT_OK);
        finish();
    }

    public void finishWithResultCodeAndIntent(View view){
        Intent intent = new Intent();
        setResult(RESULT_OK, intent);
        finish();
    }

    public void finishWithContent(View view){
        Intent intent = new Intent("com.hormiga6.androidapipractice.test");
        intent.putExtra("hoge","fuga");
        setResult(RESULT_OK, intent);
        finish();
    }

}