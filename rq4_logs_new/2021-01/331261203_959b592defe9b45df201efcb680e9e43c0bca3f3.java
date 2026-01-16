package com.example.popcat;

import androidx.appcompat.app.AppCompatActivity;

import android.app.Application;
import android.content.Intent;
import android.os.Bundle;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;

public class StoreActivity extends AppCompatActivity {

    TextView money;
    Button mainbutton;
    Button buy;

    int counter;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_store);

        Intent intent = getIntent();

        money = findViewById(R.id.money);
        mainbutton = findViewById(R.id.mainbutton);
        buy = findViewById(R.id.buy);

        counter = intent.getIntExtra("popcount",0);
        money.setText("POP : "+counter);

        mainbutton.setOnClickListener(v -> {
            Intent intent_result = new Intent();
            intent_result.putExtra("result", counter);
            setResult(2001,intent_result);
            finish();
        });

        buy.setOnClickListener(v -> {
            if(counter<10){
                Toast.makeText(StoreActivity.this, "Need more pop", Toast.LENGTH_SHORT).show();
            }else{
                counter-=10;
                money.setText("POP : "+counter);
            }
        });
    }
}