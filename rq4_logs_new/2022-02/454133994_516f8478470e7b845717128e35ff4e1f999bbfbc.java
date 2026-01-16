package com.codewithArdents.dysgraphia;

import androidx.appcompat.app.AppCompatActivity;
import androidx.cardview.widget.CardView;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Toast;

public class homepage extends AppCompatActivity {

    CardView cv1 = findViewById(R.id.cardView1);
    CardView cv2 = findViewById(R.id.cardView2);
    CardView cv3 = findViewById(R.id.cardView3);

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_homepage);



        cv1.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Toast.makeText(getApplicationContext(),"clicked",Toast.LENGTH_SHORT).show();
                cv1.getContext().startActivity(new Intent(cv1.getContext(), alpha_home.class));
            }
        });
        cv2.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Toast.makeText(getApplicationContext(),"clicked",Toast.LENGTH_SHORT).show();
                cv2.getContext().startActivity(new Intent(cv2.getContext(), alpha_home.class));
            }
        });
        cv3.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Toast.makeText(getApplicationContext(),"clicked",Toast.LENGTH_SHORT).show();
                cv3.getContext().startActivity(new Intent(cv3.getContext(), num_home.class));
            }
        });




    }
}