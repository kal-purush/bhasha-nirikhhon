package com.tutorial.travel.Activity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;

import com.tutorial.travel.DAOs.UserDAO;
import com.tutorial.travel.Database.UserDatabase;
import com.tutorial.travel.Entity.User;
import com.tutorial.travel.R;

import java.util.ArrayList;
import java.util.List;

public class LogInActivity extends AppCompatActivity {
    private UserDatabase db;
    private UserDAO dao;
    public Button submitButton;
    public EditText userText;
    public EditText passText;

    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_login);

        submitButton = findViewById(R.id.btnSubmit);
        userText = findViewById(R.id.editUser);
        passText = findViewById(R.id.editPassword);

        dao = UserDatabase.getInstance(this).userDAO();

        submitButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                onLogin(view);
            }
        });
    }

    private void onLogin(View view) {
        String username = userText.getText().toString();
        String password = passText.getText().toString();

        if (username.equals("admin") && password.equals("Admin@123")) {
            Toast.makeText(getApplicationContext(), "Logisuccessful", Toast.LENGTH_SHORT).show();
            Intent intent = new Intent(this, MainActivity.class);
            startActivity(intent);
        } else {
            Toast.makeText(getApplicationContext(), "Login failed", Toast.LENGTH_SHORT).show();
        }
    }
}