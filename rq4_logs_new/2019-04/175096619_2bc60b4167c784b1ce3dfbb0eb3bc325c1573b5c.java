package com.example.triazystaapp;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import java.util.Objects;

public class LogInActivity extends AppCompatActivity {
    EditText username;
    Button login;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_log_in);
        username=findViewById(R.id.username);
        login=findViewById(R.id.login);
        login.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                if(Objects.equals(username.getText().toString(), "admin"))
                {   Intent intent = new Intent(LogInActivity.this, MapsActivity.class);
                    Toast.makeText(LogInActivity.this,"You have Authenticated Successfully",Toast.LENGTH_LONG).show();
                    startActivity(intent);
                }else
                {
                    Toast.makeText(LogInActivity.this,"Authentication Failed",Toast.LENGTH_LONG).show();
                }
            }
        });
    }
}