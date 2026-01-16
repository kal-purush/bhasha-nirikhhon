package com.example.taskmaster;

import static com.example.taskmaster.Addtask.TAG;

import androidx.appcompat.app.AppCompatActivity;

import android.annotation.SuppressLint;
import android.content.Intent;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import com.amplifyframework.core.Amplify;
import com.google.android.material.snackbar.Snackbar;

public class Login_page extends AppCompatActivity {
    EditText email;
    EditText password;

    @SuppressLint("MissingInflatedId")
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_login_page);


        Button loginButton = (Button)findViewById(R.id.SigninB);
        Button SignUpButton = (Button)findViewById(R.id.SignupB);
        SignUpButton.setOnClickListener(new View.OnClickListener()  {
            @Override
            public void onClick(View view) {
                Intent go = new Intent(Login_page.this, Signup.class);
                startActivity(go);
            }
        });

        loginButton.setOnClickListener(v ->
        {       email = ((EditText) findViewById(R.id.emaillog));
            String emailstring = email.getText().toString();

            password = ((EditText) findViewById(R.id.pass));
            String passwordstring = password.getText().toString();
            Amplify.Auth.signIn(emailstring,passwordstring
                ,
                success ->
                {     Snackbar.make(findViewById(R.id.loginpage), "Login succeeded", Snackbar.LENGTH_SHORT).show();

                    Intent goToVerifyIntent= new Intent(Login_page.this, MainActivity.class);
                    startActivity(goToVerifyIntent);
                    Log.i(TAG, "Login succeeded: "+success.toString());
                },
                failure ->
                {
                    Snackbar.make(findViewById(R.id.loginpage), "Login failed", Snackbar.LENGTH_SHORT).show();

                    Log.i(TAG, "Login failed: "+failure.toString());
                }
        );
        });
    }
}