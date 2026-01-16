package com.example.travelerreservation;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

import com.example.travelerreservation.managers.ContextManager;
import com.example.travelerreservation.managers.LogInManager;

public class LogIn extends AppCompatActivity {

    LogInManager logInManager;
    EditText emailEditText;
    EditText passwordEditText;

    TextView signUpLink;

    Button loginBtn;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_log_in);

        ContextManager.getInstance().setApplicationContext(getApplicationContext());
        logInManager = LogInManager.getInstance();

        this.emailEditText = findViewById(R.id.loginEmailEditText);
        this.passwordEditText = findViewById(R.id.loginPasswordEditText);
        this.signUpLink = findViewById(R.id.signUpLink);
        this.loginBtn = findViewById(R.id.logInBtn);

        this.signUpLink.setOnClickListener(view -> goToSignUp());

        this.loginBtn.setOnClickListener(view -> login());

    }

    private void login(){
        logInManager.login(
                emailEditText.getText().toString(),
                passwordEditText.getText().toString(),
                () -> handleLoginSuccess(),
                error -> handleLoginFailed(error));
    }

    private void handleLoginSuccess(){
        //logInManager.setLoggedInState(true);
        Toast.makeText(this, "SUCCESS", Toast.LENGTH_LONG).show();
        Intent intent = new Intent(this, MainActivity.class);
        startActivity(intent);
        finish();
    }


    private void handleLoginFailed(String error){
        Toast.makeText(this, error, Toast.LENGTH_LONG).show();
    }

    private void goToSignUp() {
        Intent intent = new Intent(this, TravelerSignUp.class);
        startActivity(intent);
        //finish();
    }

}