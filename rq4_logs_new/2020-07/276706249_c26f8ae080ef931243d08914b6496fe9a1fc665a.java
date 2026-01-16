package com.dtavana.foodswipe.activities;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.util.Log;

import com.dtavana.foodswipe.databinding.ActivityLoginBinding;
import com.parse.ParseUser;

public class LoginActivity extends AppCompatActivity {

    public static final String TAG = "LoginActivity";

    ActivityLoginBinding binding;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        binding = ActivityLoginBinding.inflate(getLayoutInflater());
        setContentView(binding.getRoot());

        if(ParseUser.getCurrentUser() != null) {
            Log.i(TAG, "onCreate: User already logged in, skipping login screen");
//            goMainActivity();
        }
    }

//    private void goMainActivity() {
//        Intent i = new Intent(this, MainActivity.class);
//        startActivity(i);
//        Objects.requireNonNull(getActivity()).finish();
//    }
}