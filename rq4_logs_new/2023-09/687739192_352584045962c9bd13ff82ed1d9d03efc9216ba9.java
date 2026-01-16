package org.welcomedhere.welcomed;

import static androidx.constraintlayout.helper.widget.MotionEffect.TAG;

import android.content.Intent;
import android.content.pm.ActivityInfo;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.Task;
import com.google.firebase.auth.AuthResult;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.auth.FirebaseUser;

import org.welcomedhere.welcomed.data.ProfileManager;

public class SignupActivity extends AppCompatActivity {

    private FirebaseAuth mAuth;

    @Override
    public void onCreate(Bundle savedInstanceState) {
        // initialize content view and orientation
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_signup);
        setRequestedOrientation(ActivityInfo.SCREEN_ORIENTATION_PORTRAIT);

        // Initialize Firebase Auth
        mAuth = FirebaseAuth.getInstance();

        // send user back to log in screen if requested
        final TextView login = (TextView) this.findViewById(R.id.login);
        login.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(v.getContext(), LoginActivity.class);
                startActivity(intent);
                finish();
            }
        });

        // create a listener for the signUp button
        Button signUp = findViewById(R.id.signUp);
        signUp.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                // get the email
                String email = ((EditText)findViewById(R.id.email)).getText().toString();
                // get the name
                String name = ((EditText)findViewById(R.id.first_name)).getText().toString();

                // check that password and confirm password match
                if(((EditText)findViewById(R.id.password)).getText().toString().equals(((EditText)findViewById(R.id.confirm_password)).getText().toString()))
                {
                    // get password
                    String password = ((EditText)findViewById(R.id.password)).getText().toString();

                    // create a user account with data
                    mAuth.createUserWithEmailAndPassword(email, password)
                            .addOnCompleteListener(SignupActivity.this, new OnCompleteListener<AuthResult>() {
                                @Override
                                public void onComplete(@NonNull Task<AuthResult> task) {
                                    if (task.isSuccessful()) {
                                        // Sign in success, update UI with the signed-in user's information
                                        Log.d(TAG, "createUserWithEmail:success");
                                        FirebaseUser user = mAuth.getCurrentUser();

                                        // get userID
                                        String userID = user.getUid();

                                        // create a new user using firebase id and update to the database
                                        User newUser = new User(userID, name, email);
                                        User result = ProfileManager.updateUser(newUser);

                                        Toast.makeText(SignupActivity.this, "Account successfully created",
                                                Toast.LENGTH_SHORT).show();

                                        // on successful account creation, send a verification email to user and send them to the login page
                                        Intent intent = new Intent(getApplicationContext(), LoginActivity.class);
                                        startActivity(intent);
                                        finish();
                                    } else {
                                        // If sign in fails, display a message to the user.
                                        Log.w(TAG, "createUserWithEmail:failure", task.getException());
                                        Toast.makeText(SignupActivity.this, "Invalid email",
                                                Toast.LENGTH_SHORT).show();
                                    }
                                }
                            });
                }
                // if they don't, notify user
                else
                {
                    Toast.makeText(SignupActivity.this, "Password does not match.",
                            Toast.LENGTH_SHORT).show();
                }
            }
        });
    }
}