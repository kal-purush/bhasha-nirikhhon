package com.example.travelerreservation;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import com.example.travelerreservation.managers.ContextManager;
import com.example.travelerreservation.managers.SignUpManager;
import com.example.travelerreservation.models.SignUpService;

public class TravelerSignUp extends AppCompatActivity {
    EditText nicEditText;
    EditText firstNameEditText;
    EditText lastNameEditText;
    EditText dateOfBirthEditText;
    EditText phoneEditText;
    EditText emailEditText;
    EditText passwordEditText;
    EditText confPasswordEditText;
    Button singUpBtn;

    SignUpManager signUpManager;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_traveler_sign_up);
        ContextManager.getInstance().setApplicationContext(getApplicationContext());
        this.signUpManager = SignUpManager.getInstance();
        this.nicEditText = findViewById(R.id.nicEditText);
        this.firstNameEditText = findViewById(R.id.firstNameEditText);
        this.lastNameEditText = findViewById(R.id.lastNameEditText);
        this.dateOfBirthEditText = findViewById(R.id.dateOfBirthEditText);
        this.phoneEditText = findViewById(R.id.phoneEditText);
        this.emailEditText = findViewById(R.id.emailEditText);
        this.passwordEditText = findViewById(R.id.passwordEditText);
        this.confPasswordEditText = findViewById(R.id.confPasswordEditText);
        this.singUpBtn = findViewById(R.id.signUpBtn);
        this.singUpBtn.setOnClickListener(view -> signUp());
    }

    private void signUp() {
        String nic = this.nicEditText.getText().toString();
        String firstName = this.firstNameEditText.getText().toString();
        String lastName = this.lastNameEditText.getText().toString();
        String dateOfBirth = this.dateOfBirthEditText.getText().toString();
        String phoneNo = this.phoneEditText.getText().toString();
        String email = this.emailEditText.getText().toString();
        String password = this.passwordEditText.getText().toString();
        String confPassword = this.confPasswordEditText.getText().toString();

        if (!nic.isEmpty() && !firstName.isEmpty() && !lastName.isEmpty() && !phoneNo.isEmpty() && !dateOfBirth.isEmpty() && !email.isEmpty()
                && !password.isEmpty() && !confPassword.isEmpty()) {
            if (confPassword.equals(password)) {
                signUpManager.signUp(nic, firstName, lastName, dateOfBirth, phoneNo, email, password, () -> handleSignUpSuccessful(),
                        error -> handleSignUpFailed(error)
                );
            } else {
                Toast.makeText(this, "Passwords do not match", Toast.LENGTH_LONG).show();
            }
        } else {
            Toast.makeText(this, "Please fill all the fields!", Toast.LENGTH_LONG).show();
        }
    }

    private void handleSignUpSuccessful(){
        Toast.makeText(this, "Successful!", Toast.LENGTH_LONG).show();
    }

    private void handleSignUpFailed(String error){
        Toast.makeText(this, error, Toast.LENGTH_LONG).show();
    }
}