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
import com.example.prm_shop.activities.ProductActivity;
import com.example.prm_shop.activities.UserInformationActivity;
import com.example.prm_shop.models.request.ChangePasswordRequest;
import com.example.prm_shop.network.ApiClient;
import com.example.prm_shop.network.MemberService;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class ChangePasswordActivity extends AppCompatActivity {

    private EditText editCurrentPassword, editNewPassword;
    private Button buttonChangePassword, buttonCancel;
    private ImageView imageHome;
    private MemberService memberService;
    private int userId;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_change_password);

        try {
            editCurrentPassword = findViewById(R.id.editCurrentPassword);
            editNewPassword = findViewById(R.id.editNewPassword);
            buttonChangePassword = findViewById(R.id.changePasswordButton);
            buttonCancel = findViewById(R.id.cancelButton);
            imageHome = findViewById(R.id.imageHome);

            memberService = ApiClient.getRetrofitInstance().create(MemberService.class);

            String token = getToken();

            if (!token.isEmpty()) {
                JWT jwt = new JWT(token);
                String userIdStr = jwt.getClaim("memberId").asString();
                if (userIdStr != null && !userIdStr.isEmpty()) {
                    userId = Integer.parseInt(userIdStr);
                } else {
                    Toast.makeText(this, "User ID is missing", Toast.LENGTH_SHORT).show();
                    finish();
                }
            } else {
                Toast.makeText(this, "Token not found", Toast.LENGTH_SHORT).show();
                finish();
            }

            buttonChangePassword.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    changePassword();
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
            Log.e("ChangePasswordActivity", "Error during onCreate: " + e.getMessage());
            Toast.makeText(this, "Initialization error", Toast.LENGTH_SHORT).show();
            finish();
        }
    }


    private void changePassword() {
        try {
            String currentPassword = editCurrentPassword.getText().toString().trim();
            String newPassword = editNewPassword.getText().toString().trim();

            ChangePasswordRequest request = new ChangePasswordRequest();
            request.setCurrentPassword(currentPassword);
            request.setNewPassword(newPassword);

            Call<Void> call = memberService.changePassword(userId, request);
            call.enqueue(new Callback<Void>() {
                @Override
                public void onResponse(Call<Void> call, Response<Void> response) {
                    if (response.isSuccessful()) {
                        Toast.makeText(ChangePasswordActivity.this, "Password changed successfully", Toast.LENGTH_SHORT).show();
                        navigateToUserInfo();  // Navigate back to user information page
                    } else {
                        handleErrorResponse(response);
                    }
                }

                @Override
                public void onFailure(Call<Void> call, Throwable t) {
                    Log.e("ChangePasswordActivity", "Error: " + t.getMessage(), t);
                    Toast.makeText(ChangePasswordActivity.this, "Failed to change password", Toast.LENGTH_SHORT).show();
                }
            });
        } catch (Exception e) {
            Log.e("ChangePasswordActivity", "Error during changePassword: " + e.getMessage(), e);
            Toast.makeText(this, "Change password error", Toast.LENGTH_SHORT).show();
        }
    }

    private void handleErrorResponse(Response<Void> response) {
        // Handle error response from server
        // Example: Logging error message or displaying error to user
        if (response.errorBody() != null) {
            try {
                String errorBody = response.errorBody().string();
                Log.e("ChangePasswordActivity", "Error response: " + errorBody);
                Toast.makeText(this, "Failed to change password: " + errorBody, Toast.LENGTH_SHORT).show();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            Toast.makeText(this, "Failed to change password", Toast.LENGTH_SHORT).show();
        }
    }

    private String getToken() {
        SharedPreferences sharedPreferences = getSharedPreferences("MyPrefs", MODE_PRIVATE);
        return sharedPreferences.getString("TOKEN", "");
    }

    private void navigateToUserInfo() {
        Intent intent = new Intent(ChangePasswordActivity.this, UserInformationActivity.class);
        startActivity(intent);
        finish();
    }

    private void navigateToBlankActivity() {
        Intent intent = new Intent(ChangePasswordActivity.this, ProductActivity.class);
        startActivity(intent);
        finish();
    }
}