package vn.edu.usth.outlook.activities;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;

import com.google.android.material.button.MaterialButton;

import vn.edu.usth.outlook.R;
import vn.edu.usth.outlook.database.DatabaseHelper;

public class SignupActivity extends AppCompatActivity {

    private EditText newEmail, phoneNumber, username, password, confirmPassword;
    private TextView usernameError;
    private TextView invalidEmailError, invalidPhoneNumber, invalidConfirmPass, invalidPassword;


    private DatabaseHelper databaseHelper;  // Instance of DatabaseHelper

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_signup); // Set layout for SignupActivity

        // Initialize database helper
        databaseHelper = new DatabaseHelper(this);

        // Initialize views
        newEmail = findViewById(R.id.newEmail);
        phoneNumber = findViewById(R.id.phoneNumber);
        username = findViewById(R.id.username);
        password = findViewById(R.id.password);
        confirmPassword = findViewById(R.id.confirmPassword);

        // Initialize error views
        usernameError = findViewById(R.id.usernameError);
        invalidEmailError = findViewById(R.id.invalidEmailError);
        invalidPhoneNumber = findViewById(R.id.invalidPhoneNumber);
        invalidConfirmPass = findViewById(R.id.invalidConfirmPass);
        invalidPassword = findViewById(R.id.invalidPassword);

        MaterialButton signupButton = findViewById(R.id.signupButton);

        // Set click listener for signup button
        signupButton.setOnClickListener(v -> validateAndSignUp());

        // Set click listener for going to login
        TextView goToLogin = findViewById(R.id.loginText);
        goToLogin.setOnClickListener(this::goToLogin);
    }

    // Method to validate fields and sign up
    private void validateAndSignUp() {
        boolean isValid = true;

        // Reset all error messages to GONE
        resetErrorMessages();

        String emailInput = newEmail.getText().toString().trim();

        // Check email
        if (emailInput.isEmpty()) {
            invalidEmailError.setText("Please input email");
            invalidEmailError.setVisibility(View.VISIBLE);
            isValid = false;
        } else if (!isValidEmail(emailInput)) {
            invalidEmailError.setText("Invalid email format");
            invalidEmailError.setVisibility(View.VISIBLE);
            isValid = false;
        } else if (databaseHelper.checkEmailExists(emailInput)) {  // Check if email already exists
            invalidEmailError.setText("Email already exists!");
            invalidEmailError.setVisibility(View.VISIBLE);
            isValid = false;
        }

        // Check phone number
        String phoneInput = phoneNumber.getText().toString().trim();
        if (phoneInput.isEmpty()) {
            invalidPhoneNumber.setText("Please input phonenumber");
            invalidPhoneNumber.setVisibility(View.VISIBLE);
            isValid = false;
        } else if (!isValidPhone(phoneInput)) {
            invalidPhoneNumber.setText("Invalid phone number");
            invalidPhoneNumber.setVisibility(View.VISIBLE);
            isValid = false;
        } else if (databaseHelper.checkPhoneExists(phoneInput)) {  // Check if phone number already exists
            invalidPhoneNumber.setText("Phonenumber already exists!");
            invalidPhoneNumber.setVisibility(View.VISIBLE);
            isValid = false;
        }

        // Check username
        String usernameInput = username.getText().toString().trim();
        if (usernameInput.isEmpty()) {
            usernameError.setText("Please input username");
            usernameError.setVisibility(View.VISIBLE);
            isValid = false;
        } else if (databaseHelper.checkUsernameExists(usernameInput)) {
            usernameError.setText("Username already exists!");
            usernameError.setVisibility(View.VISIBLE);
            isValid = false;
        }

        // Check password
        String passwordInput = password.getText().toString().trim();
        if (passwordInput.isEmpty()) {
            invalidPassword.setText("Please input password");
            invalidPassword.setVisibility(View.VISIBLE);
            isValid = false;
        } else if (!isValidPassword(passwordInput)) {
            invalidPassword.setText("Invalid password format"); // Thêm thông báo cụ thể cho trường hợp này
            invalidPassword.setVisibility(View.VISIBLE);
            isValid = false;
        }

// Check confirm password
        String confirmPassInput = confirmPassword.getText().toString().trim();
        if (confirmPassInput.isEmpty()) {
            invalidConfirmPass.setText("Please confirm your password");
            invalidConfirmPass.setVisibility(View.VISIBLE);
            isValid = false;
        } else if (!confirmPassInput.equals(passwordInput)) {
            invalidConfirmPass.setText("Passwords do not match"); // Cập nhật thông báo cho trường hợp này
            invalidConfirmPass.setVisibility(View.VISIBLE);
            isValid = false;
        }


        // If all fields are valid, insert user into database
        if (isValid) {
            boolean isInserted = databaseHelper.insertUser(emailInput, usernameInput, phoneInput, passwordInput);
            if (isInserted) {
                Toast.makeText(this, "Registration successful!", Toast.LENGTH_SHORT).show();
                Intent intent = new Intent(SignupActivity.this, LoginActivity.class);
                startActivity(intent);
                finish(); // Finish the activity to remove it from the back stack
            } else {
                Toast.makeText(this, "Registration failed!", Toast.LENGTH_SHORT).show();
            }
        }
    }

    private void resetErrorMessages() {
        // Reset all error messages into default text and hide them
        invalidEmailError.setText("");
        invalidEmailError.setVisibility(View.GONE);

        invalidPhoneNumber.setText("");
        invalidPhoneNumber.setVisibility(View.GONE);

        usernameError.setText("");
        usernameError.setVisibility(View.GONE);

        invalidPassword.setText("");
        invalidPassword.setVisibility(View.GONE);

        invalidConfirmPass.setText("");
        invalidConfirmPass.setVisibility(View.GONE);
    }




    public void goToLogin(View view) {
        Intent intent = new Intent(SignupActivity.this, LoginActivity.class);
        startActivity(intent);
        finish(); // Optionally finish this activity
    }

    private boolean isValidEmail(String email) {
        String emailPattern = "[a-zA-Z0-9._-]+@[a-z]+\\.+[a-z]+";
        return email.matches(emailPattern);
    }

    private boolean isValidPhone(String phone) {
        String phonePattern = "^\\d{10,11}$";
        return phone.matches(phonePattern);
    }

    private boolean isValidPassword(String password) {
        String passwordPattern = "^(?=.*[A-Z])(?=.*\\d)(?=.*[@$!%*?&])[A-Za-z\\d@$!%*?&]{8,}$";
        return password.matches(passwordPattern);
    }
}