package com.example.enigmator.activity;

import android.os.Bundle;
import android.util.Log;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import com.example.enigmator.R;
import com.example.enigmator.controller.HttpRequest;
import com.example.enigmator.entity.Response;

import lombok.AllArgsConstructor;

import static com.example.enigmator.controller.HttpRequest.POST;

public class RegisterActivity extends HttpActivity {
    private static final String TAG = RegisterActivity.class.getName();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_register);

        final EditText editEmail = findViewById(R.id.edit_email);
        final EditText editUsername = findViewById(R.id.edit_username);
        final EditText editFirstName = findViewById(R.id.edit_first_name);
        final EditText editLastName = findViewById(R.id.edit_last_name);
        final EditText editPassword = findViewById(R.id.edit_password);
        final EditText editCountry = findViewById(R.id.edit_country);

        final Button btnSubmit = findViewById(R.id.btn_submit);
        btnSubmit.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if (!isInternetConnected()) {
                    buildNoConnectionErrorDialog(null);
                } else {
                    String email = editEmail.getText().toString();
                    String username = editUsername.getText().toString();
                    String firstName = editFirstName.getText().toString();
                    String lastName = editLastName.getText().toString();
                    String password = editPassword.getText().toString();
                    String country = editCountry.getText().toString();

                    if (email.isEmpty() || username.isEmpty() || firstName.isEmpty()
                            || lastName.isEmpty() || password.isEmpty() || country.isEmpty()) {
                        Toast.makeText(RegisterActivity.this, R.string.toast_invalid_form, Toast.LENGTH_SHORT).show();
                    } else {
                        final CreatedUser newUser = new CreatedUser(email, username, firstName, lastName, password, country);

                        httpManager.addToQueue(POST, "/UserEnigmators", gson.toJson(newUser), new HttpRequest.HttpRequestListener() {
                            @Override
                            public void prepareRequest() {
                                btnSubmit.setEnabled(false);
                            }

                            @Override
                            public void handleSuccess(Response response) {
                                Toast.makeText(RegisterActivity.this, R.string.toast_account_created, Toast.LENGTH_SHORT).show();
                                finish();
                            }

                            @Override
                            public void handleError(Response error) {
                                btnSubmit.setEnabled(true);
                                Log.e(TAG, "/UserEnigmators : " + gson.toJson(newUser));
                                Log.e(TAG, error.toString());
                            }
                        });
                    }
                }
            }
        });
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        if (item.getItemId() == android.R.id.home) {
            onBackPressed();
        }
        return true;
    }

    @AllArgsConstructor
    private class CreatedUser {
        private final String email;
        private final String username;
        private final String firstName;
        private final String lastName;
        private final String password;
        private final String country;
    }
}