package com.example.eventz;

import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.OnFailureListener;
import com.google.android.gms.tasks.Task;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.auth.FirebaseUser;


public class Settings extends AppCompatActivity {
    TextView message;
    EditText emailEditText, passwordEditText;
    Button email_button, password_button;
    FirebaseAuth mFirebaseAuth;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.settings_activity);

        mFirebaseAuth = FirebaseAuth.getInstance();

        emailEditText = (EditText)findViewById(R.id.edit_email);
        passwordEditText = (EditText)findViewById(R.id.edit_password);
        message = (TextView)findViewById(R.id.message);

        email_button = (Button)findViewById(R.id.save_email);
        password_button = (Button)findViewById(R.id.save_password);

        message.setText("These operations are sensible and they may require re-authentification if " +
                "you have been logged in for too long. If so, please logout and login again.");

        password_button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                String new_psswd = passwordEditText.getText().toString();

                if(new_psswd.isEmpty()) {
                    passwordEditText.setError("Please enter new password");
                    passwordEditText.requestFocus();
                }
                else {
                    FirebaseUser user = FirebaseAuth.getInstance().getCurrentUser();
                    user.updatePassword(new_psswd)
                            .addOnCompleteListener(new OnCompleteListener<Void>() {
                                @Override
                                public void onComplete(@NonNull Task<Void> task) {
                                    if (task.isSuccessful()) {
                                        Toast toast = Toast.makeText(getApplicationContext(), "User password updated.", Toast.LENGTH_LONG);
                                        toast.show();
                                    }
                                }
                            })
                    .addOnFailureListener(new OnFailureListener() {
                        @Override
                        public void onFailure(@NonNull Exception e) {
                            Toast toast = Toast.makeText(getApplicationContext(), e.getMessage(), Toast.LENGTH_LONG);
                            toast.show();
                        }
                    });
                }
            }
        });

        email_button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                String new_email = emailEditText.getText().toString();

                if(new_email.isEmpty()) {
                    emailEditText.setError("Please enter new email");
                    emailEditText.requestFocus();
                }
                else {

                    FirebaseUser user = FirebaseAuth.getInstance().getCurrentUser();
                    user.updateEmail(new_email)
                            .addOnFailureListener(new OnFailureListener() {
                                @Override
                                public void onFailure(@NonNull Exception e) {
                                    Toast toast = Toast.makeText(getApplicationContext(), e.getMessage(), Toast.LENGTH_LONG);
                                    toast.show();
                                }
                            })
                            .addOnCompleteListener(new OnCompleteListener<Void>() {
                                @Override
                                public void onComplete(@NonNull Task<Void> task) {
                                    if (task.isSuccessful()) {
                                        Toast toast = Toast.makeText(getApplicationContext(), "User email address updated.", Toast.LENGTH_LONG);
                                        toast.show();
                                    }
                                }
                            });
                }
            }
        });
    }
}