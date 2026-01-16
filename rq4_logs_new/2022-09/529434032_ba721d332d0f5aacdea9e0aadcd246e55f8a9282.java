package com.example.firebasegsocapp.activity;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.DialogInterface;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;
import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;
import com.example.firebasegsocapp.R;
import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.OnFailureListener;
import com.google.android.gms.tasks.Task;
import com.google.android.material.textfield.TextInputEditText;
import com.google.firebase.auth.FirebaseAuth;
import org.jetbrains.annotations.NotNull;

import static com.example.firebasegsocapp.activity.MainActivity.getFirebaseAuth;

public class ResetPasswordActivity extends AppCompatActivity {

    private TextInputEditText edtTextResetEmail;
    private Button btnResetPassword;
    private TextView txtViewCancel;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_reset_password);

        init();
        setListeners();
    }

    private void init(){
        edtTextResetEmail = findViewById(R.id.edtTextResetEmail);
        btnResetPassword = findViewById(R.id.buttonReset);
        txtViewCancel = findViewById(R.id.txtViewCancelReset);
    }

    private void setListeners(){
        txtViewCancel.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                finish();
            }
        });

        btnResetPassword.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String email = edtTextResetEmail.getText().toString();
                if(!email.isEmpty()){
                    FirebaseAuth auth = FirebaseAuth.getInstance();

                    auth.sendPasswordResetEmail(email)
                            .addOnCompleteListener(new OnCompleteListener<Void>() {
                                @Override
                                public void onComplete(@NonNull Task<Void> task) {
                                    if (task.isSuccessful()) {
                                        AlertDialog.Builder builder = new AlertDialog.Builder(ResetPasswordActivity.this);
                                        builder.setTitle("E-mail Verification")
                                                .setMessage("An E-mail password reset was sent to " + email + ".")
                                                .setCancelable(false)
                                                .setPositiveButton("OK", new DialogInterface.OnClickListener() {
                                                    @Override
                                                    public void onClick(DialogInterface dialog, int which) {
                                                        finish();
                                                    }
                                                });
                                        AlertDialog alert = builder.create();
                                        alert.show();
                                    }
                                }
                            })
                            .addOnFailureListener(new OnFailureListener() {
                                @Override
                                public void onFailure(@NonNull @NotNull Exception e) {
                                    edtTextResetEmail.setError(e.getLocalizedMessage());
                                }
                            });
                }else{
                    edtTextResetEmail.setError("E-mail can not be empty");
                }
            }
        });


    }

}