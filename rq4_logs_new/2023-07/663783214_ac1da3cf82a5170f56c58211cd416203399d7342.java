package com.example.jalihara;

import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.view.inputmethod.InputMethodManager;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import androidx.appcompat.app.AppCompatActivity;

public class LoginActivity extends AppCompatActivity {

    EditText usernameEditText;
    EditText passwordEditText;

    Button loginBtn;

    TextView errorTextView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_login);

        usernameEditText = findViewById(R.id.username_input);
        passwordEditText = findViewById(R.id.password_input);
        loginBtn = findViewById(R.id.login_button);
        errorTextView = findViewById(R.id.error_txt);

        loginBtn.setOnClickListener(view ->{
            String username = usernameEditText.getText().toString();
            String password = passwordEditText.getText().toString();

            if (username.equals("")) {
                errorTextView.setVisibility(View.VISIBLE);
                errorTextView.setText("username cannot be empty");
            } else if (password.equals("")) {
                errorTextView.setVisibility(View.VISIBLE);
                errorTextView.setText("password cannot be empty");
            } else if (username.length() <= 5) {
                errorTextView.setVisibility(View.VISIBLE);
                errorTextView.setText("username must be more then 5 characters");
            } else if (password.length() <= 5) {
                errorTextView.setVisibility(View.VISIBLE);
                errorTextView.setText("password must be more than 5 characters");
            } else {
                ((UsernameGlobal) this.getApplication()).setUsername(username);
                startActivity(new Intent(LoginActivity.this, MainActivity.class));
            }

            InputMethodManager imm = (InputMethodManager)getSystemService(Context.INPUT_METHOD_SERVICE);
            imm.hideSoftInputFromWindow(view.getWindowToken(), 0);
        });
    }
}