package com.abdev.offlinephonefinder;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

public class ChangePasswordActivity extends AppCompatActivity {

    DatabaseHelper db;
    EditText oldPasswordET, newPasswordET, confirmNewPasswordET;
    Button changePasswordB;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_change_password);

        db = new DatabaseHelper(this);
        oldPasswordET = findViewById(R.id.oldPassword);
        newPasswordET = findViewById(R.id.newPassword);
        confirmNewPasswordET = findViewById(R.id.confirmNewPassword);
        changePasswordB = findViewById(R.id.changePassword);

        changePasswordB.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                String oldPassTyped = oldPasswordET.getText().toString();
                String newPass = newPasswordET.getText().toString();
                String confirmNewPass = confirmNewPasswordET.getText().toString();

                SharedPreferences preferences = getSharedPreferences("prefs", MODE_PRIVATE);
                String oldPassword = preferences.getString("password", null);

                if (oldPassTyped.equals("") || newPass.equals("") || confirmNewPass.equals("")){
                    Toast.makeText(ChangePasswordActivity.this, "Please fill all fields.", Toast.LENGTH_SHORT).show();
                }
                else{
                    if (oldPassTyped.equals(oldPassword)){
                        if (newPass.equals(confirmNewPass)){
                            Boolean update = db.changePassword(newPass);
                            if (update){
                                SharedPreferences.Editor editor = preferences.edit();
                                editor.putString("password", newPass);
                                editor.apply();
                                Toast.makeText(ChangePasswordActivity.this, "Password changed successfully!", Toast.LENGTH_SHORT).show();
                                Intent i = new Intent(ChangePasswordActivity.this, MainActivity.class);
                                startActivity(i);
                                finish();
                            }
                            else{
                                Toast.makeText(ChangePasswordActivity.this, "A database error occurred.", Toast.LENGTH_SHORT).show();
                            }
                        }
                        else{
                            Toast.makeText(ChangePasswordActivity.this, "Passwords do not match.", Toast.LENGTH_SHORT).show();
                        }
                    }
                    else{
                        Toast.makeText(ChangePasswordActivity.this, "Wrong password.", Toast.LENGTH_SHORT).show();
                    }
                }
            }
        });
    }
}