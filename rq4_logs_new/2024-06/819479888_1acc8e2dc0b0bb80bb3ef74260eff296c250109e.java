package com.example.prm_shop.activities;

import android.content.SharedPreferences;
import android.os.Bundle;
import android.util.Log;
import android.widget.TextView;
import android.widget.Toast;
import androidx.appcompat.app.AppCompatActivity;

import com.auth0.android.jwt.JWT;
import com.example.prm_shop.R;
import com.example.prm_shop.models.response.UserResponse;
import com.example.prm_shop.network.ApiClient;
import com.example.prm_shop.network.MemberService;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

import android.content.SharedPreferences;
import android.os.Bundle;
import android.util.Log;
import android.widget.TextView;
import android.widget.Toast;
import androidx.appcompat.app.AppCompatActivity;
import com.auth0.android.jwt.JWT;
import com.example.prm_shop.R;
import com.example.prm_shop.models.response.UserResponse;
import com.example.prm_shop.network.ApiClient;
import com.example.prm_shop.network.MemberService;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class UserInformationActivity extends AppCompatActivity {

    private TextView userNameTextView, emailTextView, phoneTextView, addressTextView, roleTextView;
    private MemberService memberService;
    private int userId; // Changed to int for userId
    private static final String ADMIN = "Admin";
    private static final String CUSTOMER = "Customer";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_user_information);

        // Initialize TextViews
        userNameTextView = findViewById(R.id.userNameTextView);
        emailTextView = findViewById(R.id.emailTextView);
        phoneTextView = findViewById(R.id.phoneTextView);
        addressTextView = findViewById(R.id.addressTextView);
        roleTextView = findViewById(R.id.roleTextView);

        // Initialize Retrofit service
        memberService = ApiClient.getRetrofitInstance().create(MemberService.class);

        // Lấy token từ SharedPreferences
        String token = getToken();

        // Kiểm tra xem token có tồn tại không
        if (!token.isEmpty()) {
            // Giải mã token để lấy userId
            JWT jwt = new JWT(token);
            String userIdStr = jwt.getClaim("memberId").asString();
            if (userIdStr != null && !userIdStr.isEmpty()) {
                userId = Integer.parseInt(userIdStr);

                // Gọi phương thức để lấy thông tin người dùng
                getUserInformation(userId);
            } else {
                Toast.makeText(this, "User ID is missing", Toast.LENGTH_SHORT).show();
            }
        } else {
            Toast.makeText(this, "Token not found", Toast.LENGTH_SHORT).show();
        }
    }

    private void getUserInformation(int userId) { // Changed parameter type to int
        Call<UserResponse> call = memberService.getUserById(userId);
        call.enqueue(new Callback<UserResponse>() {
            @Override
            public void onResponse(Call<UserResponse> call, Response<UserResponse> response) {
                if (response.isSuccessful() && response.body() != null) {
                    UserResponse user = response.body();
                    // Update UI with user information
                    userNameTextView.setText(user.getUserName());
                    emailTextView.setText(user.getEmail());
                    phoneTextView.setText(user.getPhone());
                    addressTextView.setText(user.getAddress());
                    roleTextView.setText(getRoleName(user.getRoleId()));
                } else {
                    Toast.makeText(UserInformationActivity.this, "Failed to retrieve user information", Toast.LENGTH_SHORT).show();
                }
            }

            @Override
            public void onFailure(Call<UserResponse> call, Throwable t) {
                Log.e("UserInformationActivity", "Error: " + t.getMessage());
                Toast.makeText(UserInformationActivity.this, "An error occurred", Toast.LENGTH_SHORT).show();
            }
        });
    }

    private String getRoleName(int roleId) {
        switch (roleId) {
            case 1:
                return ADMIN;
            case 2:
                return CUSTOMER;
            default:
                return "Unknown";
        }
    }

    private String getToken() {
        // Lấy token từ SharedPreferences
        SharedPreferences sharedPreferences = getSharedPreferences("MyPrefs", MODE_PRIVATE);
        return sharedPreferences.getString("TOKEN", "");
    }
}