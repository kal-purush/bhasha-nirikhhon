package com.example.mojing;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

public class LoginActivity extends AppCompatActivity {

    private EditText usernameEditText;
    private EditText passwordEditText;
    private Button loginButton;
    private SharedPreferencesManager sharedPreferencesManager;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_login);

        sharedPreferencesManager = new SharedPreferencesManager(this);
        // 初始化视图和SharedPreferencesManager
        usernameEditText = findViewById(R.id.usernameEditText);
        passwordEditText = findViewById(R.id.passwordEditText);
        loginButton = findViewById(R.id.loginButton);

        loginButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // 处理登录按钮点击事件
                String username = usernameEditText.getText().toString();
                String password = passwordEditText.getText().toString();

                // 假设进行登录验证的逻辑
                if (isValidLogin(username, password)) {
                    // 登录成功，保存用户信息和登录状态
                    sharedPreferencesManager.setLoggedIn(true);
                    sharedPreferencesManager.setUsername(username);
                    sharedPreferencesManager.setUserVal(password);

                    Intent intent = new Intent(LoginActivity.this, MainActivity.class);
                    intent.putExtra("username", username); // 设置需要传递的数据
                    intent.putExtra("password", password); // 设置需要传递的数据
                    //intent.addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP);
                    startActivity(intent);
                    finish(); // 结束当前的LoginActivity

                    finish();
                } else {
                    Toast.makeText(LoginActivity.this, "登录失败", Toast.LENGTH_SHORT).show();
                }
            }
        });
    }

    private boolean isValidLogin(String username, String password) {
        return true;
        // 进行登录验证的逻辑，这里仅作示例
        //return username.equals("admin") && password.equals("123456");
    }
}