package co.minesweepers.maintenance;

import android.app.ProgressDialog;
import android.content.Intent;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import butterknife.Bind;
import butterknife.ButterKnife;

public class LoginActivity extends AppCompatActivity {
        private static final String TAG = "LoginActivity";
        private static final int REQUEST_SIGNUP = 0;

        @Bind(R.id.input_email) EditText mEmailText;
        @Bind(R.id.input_password) EditText mPasswordText;
        @Bind(R.id.btn_login) Button mLoginButton;

        @Override
        public void onCreate(Bundle savedInstanceState) {
            super.onCreate(savedInstanceState);
            setContentView(R.layout.activity_login);
            ButterKnife.bind(this);

            mLoginButton.setOnClickListener(new View.OnClickListener() {

                @Override
                public void onClick(View v) {
                    login();
                }
            });
        }

        public void login() {
            Log.d(TAG, "Login");

            if (!validate()) {
                onLoginFailed();
                return;
            }

            mLoginButton.setEnabled(false);

            final ProgressDialog progressDialog = new ProgressDialog(LoginActivity.this);
            progressDialog.setIndeterminate(true);
            progressDialog.setMessage("Authenticating...");
            progressDialog.show();
            progressDialog.setCancelable(false);

            final String email = mEmailText.getText().toString();
            final String password = mPasswordText.getText().toString();

            new android.os.Handler().post(
                    new Runnable() {
                        public void run() {
                            // On complete call either onLoginSuccess or onLoginFailed
                            if(email.equalsIgnoreCase("a@test.com") && password.equalsIgnoreCase("password")) {
                                Intent intent = new Intent(getApplicationContext(), MainActivity.class);
                                startActivityForResult(intent, REQUEST_SIGNUP);
                                finish();
                            } else {
                                onLoginFailed();
                            }
                            progressDialog.dismiss();
                        }
                    });
        }

        public void onLoginFailed() {
            Toast.makeText(getBaseContext(), "Login failed", Toast.LENGTH_LONG).show();

            mLoginButton.setEnabled(true);
        }

        public boolean validate() {
            boolean valid = true;

            String email = mEmailText.getText().toString();
            String password = mPasswordText.getText().toString();

            if (email.isEmpty() || !android.util.Patterns.EMAIL_ADDRESS.matcher(email).matches()) {
                mEmailText.setError("enter a valid email address");
                valid = false;
            } else {
                mEmailText.setError(null);
            }

            if (password.isEmpty() || password.length() < 4 || password.length() > 10) {
                mPasswordText.setError("between 4 and 10 alphanumeric characters");
                valid = false;
            } else {
                mPasswordText.setError(null);
            }

            return valid;
        }
    }
