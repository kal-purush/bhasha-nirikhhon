package com.delose.onlineshopph;

import android.content.Intent;
import android.support.design.widget.TextInputEditText;
import android.support.design.widget.TextInputLayout;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageView;

import org.w3c.dom.Text;

public class LoginActivity extends AppCompatActivity {

    private Button buttonLogin;
    private Button buttonRegister;
    private TextInputEditText editTextEmail;
    private TextInputEditText editTextPassword;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_login);

        //DRAW BEHIND STATUS BAR
        getWindow().getDecorView().setSystemUiVisibility(
                View.SYSTEM_UI_FLAG_LAYOUT_STABLE
                        | View.SYSTEM_UI_FLAG_LAYOUT_FULLSCREEN);

        //GET VIEWS
        buttonLogin = findViewById(R.id.button_login_login);
        buttonRegister = findViewById(R.id.button_register_login);
        editTextEmail = findViewById(R.id.text_email_login);
        editTextPassword = findViewById(R.id.text_password_login);

        //WHEN LOGIN IS CLICKED
        buttonLogin.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

            }
        });

        //WHEN CREATE ACCOUNT (REGISTER) IS CLICKED
        buttonRegister.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                //START REGISTER ACTIVITY
                Intent intentRegister = new Intent(LoginActivity.this,RegisterActivity.class);
                startActivity(intentRegister);
            }
        });
    }

    private void login()
    {

    }
}