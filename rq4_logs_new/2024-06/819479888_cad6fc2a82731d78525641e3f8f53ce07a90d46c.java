package com.example.prm_shop.activities;

import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;

import com.auth0.android.jwt.JWT;
import com.example.prm_shop.R;
import com.example.prm_shop.models.request.UpdateUserRequest;
import com.example.prm_shop.models.response.UpdateUserResponse;
import com.example.prm_shop.models.response.UserResponse;
import com.example.prm_shop.network.ApiClient;
import com.example.prm_shop.network.MemberService;

import java.io.IOException;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class UpdateUserActivity extends AppCompatActivity {

    private EditText editTextUserName, editTextEmail, editTextPhone, editTextAddress;
    private Button buttonUpdateUser, buttonCancel;
    private ImageView imageHome;
    private MemberService memberService;
    private int userId;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_update_user);

        try {
            editTextUserName = findViewById(R.id.editTextUserName);
            editTextEmail = findViewById(R.id.editTextEmail);
            editTextPhone = findViewById(R.id.editTextPhone);
            editTextAddress = findViewById(R.id.editTextAddress);
            buttonUpdateUser = findViewById(R.id.buttonUpdateUser);
            buttonCancel = findViewById(R.id.buttonCancel);
            imageHome = findViewById(R.id.imageHome);

            memberService = ApiClient.getRetrofitInstance().create(MemberService.class);

            String token = getToken();

            if (!token.isEmpty()) {
                JWT jwt = new JWT(token);
                String userIdStr = jwt.getClaim("memberId").asString();
                if (userIdStr != null && !userIdStr.isEmpty()) {
                    userId = Integer.parseInt(userIdStr);
                     getUserInformation(userId); // Uncomment if you need to prefill user information
                } else {
                    Toast.makeText(this, "User ID is missing", Toast.LENGTH_SHORT).show();
                }
            } else {
                Toast.makeText(this, "Token not found", Toast.LENGTH_SHORT).show();
            }

            buttonUpdateUser.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    updateUser();
                }
            });

            buttonCancel.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    navigateToUserInfo();
                }
            });

            imageHome.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    navigateToBlankActivity();
                }
            });
        } catch (Exception e) {
            Log.e("UpdateUserActivity", "Error during onCreate: " + e.getMessage());
            Toast.makeText(this, "Initialization error", Toast.LENGTH_SHORT).show();
        }
    }

    private void getUserInformation(int userId) {
        Call<UserResponse> call = memberService.getUserById(userId);
        call.enqueue(new Callback<UserResponse>() {
            @Override
            public void onResponse(Call<UserResponse> call, Response<UserResponse> response) {
                if (response.isSuccessful() && response.body() != null) {
                    UserResponse user = response.body();
                    editTextUserName.setText(user.getUserName());
                    editTextEmail.setText(user.getEmail());
                    editTextPhone.setText(user.getPhone());
                    editTextAddress.setText(user.getAddress());
                } else {
                    Toast.makeText(UpdateUserActivity.this, "Failed to retrieve user information", Toast.LENGTH_SHORT).show();
                }
            }

            @Override
            public void onFailure(Call<UserResponse> call, Throwable t) {
                Log.e("UpdateUserActivity", "Error: " + t.getMessage());
                Toast.makeText(UpdateUserActivity.this, "An error occurred", Toast.LENGTH_SHORT).show();
            }
        });
    }

    private void updateUser() {
        try {
            String userName = editTextUserName.getText().toString().trim();
            String email = editTextEmail.getText().toString().trim();
            String phone = editTextPhone.getText().toString().trim();
            String address = editTextAddress.getText().toString().trim();

            UpdateUserRequest request = new UpdateUserRequest();
            request.setUserName(userName);
            request.setEmail(email);
            request.setPhone(phone);
            request.setAddress(address);

            Call<UpdateUserResponse> call = memberService.updateUser(userId, request);
            call.enqueue(new Callback<UpdateUserResponse>() {
                @Override
                public void onResponse(Call<UpdateUserResponse> call, Response<UpdateUserResponse> response) {
                    if (response.isSuccessful()) {
                        UpdateUserResponse updatedUser = response.body();
                        if (updatedUser != null) {
                            Toast.makeText(UpdateUserActivity.this, "User updated successfully", Toast.LENGTH_SHORT).show();
                            navigateToUserInfo();  // Navigate back to user information page
                        } else {
                            Toast.makeText(UpdateUserActivity.this, "Empty response received", Toast.LENGTH_SHORT).show();
                        }
                    } else {
                        handleErrorResponse(response);
                    }
                }

                @Override
                public void onFailure(Call<UpdateUserResponse> call, Throwable t) {
                    Log.e("UpdateUserActivity", "Error: " + t.getMessage(), t);
                    Toast.makeText(UpdateUserActivity.this, "Failed to update user", Toast.LENGTH_SHORT).show();
                }
            });
        } catch (Exception e) {
            Log.e("UpdateUserActivity", "Error during updateUser: " + e.getMessage(), e);
            Toast.makeText(this, "Update error", Toast.LENGTH_SHORT).show();
        }
    }

    private void handleErrorResponse(Response<UpdateUserResponse> response) {
        if (response.errorBody() != null) {
            try {
                String errorBody = response.errorBody().string();
                Log.e("UpdateUserActivity", "Error response: " + errorBody);
                Toast.makeText(this, "Failed to update user: " + errorBody, Toast.LENGTH_SHORT).show();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            Toast.makeText(this, "Failed to update user", Toast.LENGTH_SHORT).show();
        }
    }

    private String getToken() {
        SharedPreferences sharedPreferences = getSharedPreferences("MyPrefs", MODE_PRIVATE);
        return sharedPreferences.getString("TOKEN", "");
    }

    private void navigateToUserInfo() {
        Intent intent = new Intent(UpdateUserActivity.this, UserInformationActivity.class);
        startActivity(intent);
        finish();
    }

    private void navigateToBlankActivity() {
        Intent intent = new Intent(UpdateUserActivity.this, BlankActivity.class);
        startActivity(intent);
        finish();
    }
}