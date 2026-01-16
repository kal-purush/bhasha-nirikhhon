package sulthon.com.iwaktrial.activity;

import android.content.Intent;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.v7.app.AppCompatActivity;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.Task;
import com.google.firebase.auth.AuthResult;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.auth.FirebaseUser;

import sulthon.com.iwaktrial.R;

public class RegisterActivity extends AppCompatActivity {

    EditText textEmail, textPassword;
    Button btntoLogin, btnRegister;

    String inputEmail, inputPassword;
    String savedEmail, savedPassword;


    private FirebaseAuth mAuth;
    private FirebaseAuth.AuthStateListener mAuthListener;

    private static final String TAG_FIREBASE = "firebase";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_register);

        textEmail = (EditText) findViewById(R.id.et_email_reg);
        textPassword = (EditText) findViewById(R.id.et_password_reg);

        btnRegister = (Button) findViewById(R.id.btn_register);
        btntoLogin = (Button) findViewById(R.id.btn_toLogin);

        mAuth = FirebaseAuth.getInstance();

        mAuthListener = new FirebaseAuth.AuthStateListener() {
            @Override
            public void onAuthStateChanged(@NonNull FirebaseAuth firebaseAuth) {
                FirebaseUser user = firebaseAuth.getCurrentUser();
                if (user != null) {
                    //user is signed in
                    Log.d(TAG_FIREBASE, "onAuthStateChanged:signed_in" + user.getUid());
                } else {
                    //user is signed out
                    Log.d(TAG_FIREBASE, "onAuthStateChanged:signed_out");
                }
            }
        };

        btnRegister.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                inputEmail = textEmail.getText().toString();
                inputPassword = textPassword.getText().toString();

                if (inputEmail.equals("") && inputPassword.equals("")) {
                    Toast.makeText(RegisterActivity.this, "Masukan email dan password anda", Toast.LENGTH_LONG)
                            .show();
                } else {
                    mAuth.createUserWithEmailAndPassword(inputEmail, inputPassword)
                            .addOnCompleteListener(RegisterActivity.this, new OnCompleteListener<AuthResult>() {
                                @Override
                                public void onComplete(@NonNull Task<AuthResult> task) {
                                    // If sign in fails, display a message to the user. If sign in succeeds
                                    // the auth state listener will be notified and logic to handle the
                                    // signed in user can be handled in the listener.
                                    if (!task.isSuccessful()) {
                                        Toast.makeText(RegisterActivity.this, "register gagal",
                                                Toast.LENGTH_SHORT).show();
                                    } else {
                                        Toast.makeText(RegisterActivity.this, "register berhasil",
                                                Toast.LENGTH_LONG).show();
                                    }
                                }
                            });
                }
            }
        });

        btntoLogin.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent toLogin = new Intent(RegisterActivity.this, LoginActivity.class);
                startActivity(toLogin);
            }
        });
    }


    @Override
    public void onStart() {
        super.onStart();
        mAuth.addAuthStateListener(mAuthListener);
    }

    @Override
    public void onStop() {
        super.onStop();
        if (mAuthListener != null) {
            mAuth.removeAuthStateListener(mAuthListener);
        }
    }
}