package com.example.travelerreservation;

import androidx.appcompat.app.AppCompatActivity;

import android.app.ProgressDialog;
import android.content.Intent;
import android.os.Bundle;
import android.util.Log;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import com.example.travelerreservation.managers.ContextManager;
import com.example.travelerreservation.managers.DatabaseManager;
import com.example.travelerreservation.managers.DeactivateManager;
import com.example.travelerreservation.managers.LogInManager;
import com.example.travelerreservation.models.UserEntity;

import java.util.List;

public class DeactivateProfile extends AppCompatActivity {

    EditText password;
    Button confirmBtn;

    DeactivateManager deactivateManager;

    DatabaseManager databaseManager;

    LogInManager logInManager;

    ProgressDialog progressDialog;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_deactivate_profile);
        ContextManager.getInstance().setApplicationContext(getApplicationContext());

        getSupportActionBar().setTitle("Deactivate Profile");
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);


        deactivateManager = DeactivateManager.getInstance();
        databaseManager = DatabaseManager.getInstance();
        logInManager = LogInManager.getInstance();

        this.password = (EditText) findViewById(R.id.deactivatePwdEditText);
        this.confirmBtn = findViewById(R.id.confirmBtn);
        this.confirmBtn.setOnClickListener(view -> onClickConfirm());

    }

    private void onClickConfirm() {
        String pwd = this.password.getText().toString().trim();

        if(pwd.isEmpty()) {
            this.displayToast("Please enter your password");
        } else {

            String nic = getIntent().getExtras().getString("nic");
            String password = getIntent().getExtras().getString("password");

                if(pwd.equals(password)) {
                    progressDialog = new ProgressDialog(this);
                    progressDialog.setMessage("Loading...");
                    progressDialog.setProgressStyle(ProgressDialog.STYLE_SPINNER);
                    progressDialog.setCancelable(false);
                    progressDialog.show();
                    deactivateManager.deactivateProfile(nic,  () -> handleUpdateSuccessful(),
                            error -> handleUpdateFailed(error));
                } else {
                    this.displayToast("Incorrect password");
                }


        }


    }

    private void displayToast(String message) {
        Toast.makeText(this, message, Toast.LENGTH_LONG).show();
    }

    private void handleUpdateSuccessful(){
        progressDialog.dismiss();
        Toast.makeText(this, "Successful!", Toast.LENGTH_LONG).show();

        this.logInManager.logout();
        Intent intent = new Intent(this, LogIn.class);
        startActivity(intent);
        finish();
    }

    private void handleUpdateFailed(String error){
        progressDialog.dismiss();
        Toast.makeText(this, error, Toast.LENGTH_LONG).show();
    }

    @Override
    public boolean onSupportNavigateUp() {
        finish();
        return true;
    }
}