package com.example.myapplication;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.util.Patterns;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.Task;
import com.google.firebase.auth.FirebaseAuth;

public class Forgotpassword_activity extends AppCompatActivity {

    private EditText EditTextEmail;
    private Button ButtonResetPassword;

    FirebaseAuth auth;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_forgotpassword_activity);
        getSupportActionBar().hide();

        EditTextEmail = (EditText) findViewById(R.id.email);
        ButtonResetPassword = (Button) findViewById(R.id.resetPassword);

        auth = FirebaseAuth.getInstance();

        ButtonResetPassword.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                resetPassword();
            }
        });
    }

    private void resetPassword(){
        String email = EditTextEmail.getText().toString().trim();

        if (email.isEmpty()){
            EditTextEmail.setError("Email is required");
            EditTextEmail.requestFocus();
            return;
        }
        if (!Patterns.EMAIL_ADDRESS.matcher(email).matches()){
            EditTextEmail.setError("Please provide valid email");
            EditTextEmail.requestFocus();
            return;
        }

        auth.sendPasswordResetEmail(email).addOnCompleteListener(new OnCompleteListener<Void>() {
            @Override
            public void onComplete(@NonNull Task<Void> task) {
                if (task.isSuccessful()){
                    Toast.makeText(Forgotpassword_activity.this,"Check your email to reset your password", Toast.LENGTH_LONG).show();
                }else{
                    Toast.makeText(Forgotpassword_activity.this,"Try again, something wrong happened", Toast.LENGTH_LONG).show();
                }
            }
        });

    }

}