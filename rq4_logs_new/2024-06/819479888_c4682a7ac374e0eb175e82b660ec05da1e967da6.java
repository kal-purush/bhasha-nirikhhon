package com.example.prm_shop.activities;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import com.example.prm_shop.MainActivity;
import com.example.prm_shop.R;
import com.example.prm_shop.models.request.RegisterUserRequest;
import com.example.prm_shop.models.response.RegisterUserResponse;
import com.example.prm_shop.network.ApiClient;
import com.example.prm_shop.network.MemberService;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class RegisterActivity extends AppCompatActivity {

    private EditText editTextUserName, editTextEmail, editTextPhone, editTextPassword, editTextAddress;
    private Button buttonRegister;
    private MemberService memberService;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_register);

        editTextUserName = findViewById(R.id.editTextUserName);
        editTextEmail = findViewById(R.id.editTextEmail);
        editTextPhone = findViewById(R.id.editTextPhone);
        editTextPassword = findViewById(R.id.editTextPassword);
        editTextAddress = findViewById(R.id.editTextAddress);
        buttonRegister = findViewById(R.id.buttonRegister);

        memberService = ApiClient.getRetrofitInstance().create(MemberService.class);

        buttonRegister.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                registerUser();
            }
        });
    }

    private void registerUser() {
        String userName = editTextUserName.getText().toString().trim();
        String email = editTextEmail.getText().toString().trim();
        String phone = editTextPhone.getText().toString().trim();
        String password = editTextPassword.getText().toString().trim();
        String address = editTextAddress.getText().toString().trim();

        RegisterUserRequest registerUserRequest = new RegisterUserRequest();
        registerUserRequest.setUserName(userName);
        registerUserRequest.setEmail(email);
        registerUserRequest.setPhone(phone);
        registerUserRequest.setPassword(password);
        registerUserRequest.setAddress(address);

        Call<RegisterUserResponse> call = memberService.register(registerUserRequest);
        call.enqueue(new Callback<RegisterUserResponse>() {
            @Override
            public void onResponse(Call<RegisterUserResponse> call, Response<RegisterUserResponse> response) {
                if (response.isSuccessful()) {
                    RegisterUserResponse registerUserResponse = response.body();
                    if (registerUserResponse != null) {
                        Toast.makeText(RegisterActivity.this, "Registration successful", Toast.LENGTH_SHORT).show();
                        Intent intent = new Intent(RegisterActivity.this, MainActivity.class);
                        startActivity(intent);
                        finish();
                    } else {
                        Toast.makeText(RegisterActivity.this, "Invalid response from server", Toast.LENGTH_SHORT).show();
                    }
                } else {
                    Log.e("RegisterActivity", "Registration failed: " + response.message());
                    Toast.makeText(RegisterActivity.this, "Registration failed: " + response.message(), Toast.LENGTH_SHORT).show();
                }
            }

            @Override
            public void onFailure(Call<RegisterUserResponse> call, Throwable t) {
                Log.e("RegisterActivity", "Error: " + t.getMessage());
                Toast.makeText(RegisterActivity.this, "Error: " + t.getMessage(), Toast.LENGTH_SHORT).show();
            }
        });
    }
}