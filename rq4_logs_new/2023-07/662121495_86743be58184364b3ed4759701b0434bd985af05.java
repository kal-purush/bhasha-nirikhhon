package com.example.mojing;

import androidx.appcompat.app.AppCompatActivity;

import android.app.AlertDialog;
import android.content.Intent;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageButton;
import android.widget.TextView;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class RegisterActivity extends AppCompatActivity {
    private int countdownTime = 60; // 倒计时时长，单位：秒

    private Handler handler = new Handler(Looper.getMainLooper());
    private Runnable countdownRunnable;
    private SharedPreferencesManager sharedPreferencesManager;
    private EditText UserPhoneEditText;
    private EditText passwordEditText;
    private Button loginButton;
    private TextView switchTextView;
    private TextView textViewRegister;
    private Button getCodeButton;
    private EditText verificationCodeEditText;
    private boolean isPasswordLogin = true; // 标志当前登录方式，初始为密码登录
    private boolean isRegister = false;     // false 登录 ture 注册


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_register);

        sharedPreferencesManager = new SharedPreferencesManager(this);
        // 初始化视图
        UserPhoneEditText = findViewById(R.id.editTextUserPhone);
        passwordEditText = findViewById(R.id.editTextPassword);
        loginButton = findViewById(R.id.buttonLogin);
        getCodeButton = findViewById(R.id.buttonGetCode);
        verificationCodeEditText = findViewById(R.id.editTextVerificationCode);
        switchTextView = findViewById(R.id.textViewSwitch);


        //返回按钮
        ImageButton btnBack = findViewById(R.id.btn_back);
        btnBack.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                finish(); // 返回上一个Activity
            }
        });
        getCodeButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String phone_number = UserPhoneEditText.getText().toString();
                if(phone_number.length()!=11){
                    showRequestFailedDialog("请输入正确的手机号码！");
                    return;
                }
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        MediaType JSON = MediaType.parse("application/json;charset=utf-8");
                        JSONObject json = new JSONObject();
                        try {
                            json.put("phone_number", phone_number);
                        } catch (JSONException e) {
                            throw new RuntimeException(e);
                        }
                        //创建一个OkHttpClient对象
                        OkHttpClient okHttpClient = new OkHttpClient();
                        RequestBody requestBody = RequestBody.create(JSON, String.valueOf(json));
                        Request request = new Request.Builder()
                                .url("http://47.102.43.156:8007/auth/code")
                                .post(requestBody)
                                .build();
                        // 发送请求并获取响应
                        try {
                            Response response = okHttpClient.newCall(request).execute();
                            // 检查响应是否成功
                            if (response.isSuccessful()) {
                                // 获取响应体
                                ResponseBody responseBody = response.body();
                                // 处理响应数据
                                String responseData = responseBody.string();
                                // 提取键为"code"的值
                                System.out.println("Response: " + responseData);
                                // 记得关闭响应体
                                responseBody.close();
                            } else {
                                // 请求失败，处理错误
                                System.out.println("Request failed");
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }).start();
                // 点击按钮后禁用按钮点击
                getCodeButton.setEnabled(false);
                // 开始倒计时
                startCountdownTimer();
            }
        });
        loginButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                //注册按钮
                String phone_number = UserPhoneEditText.getText().toString();
                String code = verificationCodeEditText.getText().toString();
                if (phone_number.equals("") || code.equals("")) {
                    return;
                }
                ;
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        MediaType JSON = MediaType.parse("application/json;charset=utf-8");
                        JSONObject json = new JSONObject();
                        try {
                            json.put("phone_number", phone_number);
                            json.put("code", code);
                        } catch (JSONException e) {
                            throw new RuntimeException(e);
                        }
                        //创建一个OkHttpClient对象
                        OkHttpClient okHttpClient = new OkHttpClient();
                        RequestBody requestBody = RequestBody.create(JSON, String.valueOf(json));
                        Request request = new Request.Builder()
                                .url("http://47.102.43.156:8007/auth/register")
                                .post(requestBody)
                                .build();
                        // 发送请求并获取响应
                        try {
                            Response response = okHttpClient.newCall(request).execute();
                            // 检查响应是否成功
                            if (response.isSuccessful()) {
                                // 获取响应体
                                ResponseBody responseBody = response.body();
                                // 处理响应数据
                                String responseData = responseBody.string();
                                JSONObject responseJson = new JSONObject(responseData);
                                // 提取键为"code"的值
                                int code = responseJson.getInt("code");
                                //确定返回状态
                                switch (code) {
                                    case 200:
                                        setData(responseJson);
                                        break;
                                    //账号已注册
                                    case 4002:
                                        runOnUiThread(new Runnable() {
                                            @Override
                                            public void run() {
                                                showRequestFailedDialog("账号已存在");
                                            }
                                        });
                                        break;
                                    //验证码错误
                                    case 1002:
                                        runOnUiThread(new Runnable() {
                                            @Override
                                            public void run() {
                                                showRequestFailedDialog("验证码错误");
                                            }
                                        });
                                        break;
                                }
                                System.out.println("Response: " + responseData);
                                // 记得关闭响应体
                                responseBody.close();
                            } else {
                                // 请求失败，处理错误
                                System.out.println("Request failed");
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (JSONException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }).start();
                // 登录成功，改变登录状态
                if (sharedPreferencesManager.isLoggedIn()) {
                    Intent intent = new Intent(RegisterActivity.this, MainActivity.class);
                    startActivity(intent);
                    finish(); // 结束当前的LoginActivity
                }
            }
        });
    }
    private void startCountdownTimer() {
        // 创建一个倒计时的Runnable
        countdownRunnable = new Runnable() {
            @Override
            public void run() {
                if (countdownTime > 0) {
                    // 设置按钮文字为剩余倒计时时间
                    getCodeButton.setText(countdownTime + "秒后重试");
                    countdownTime--;

                    // 延迟1秒后再次调用该Runnable
                    handler.postDelayed(this, 1000);
                } else {
                    // 倒计时结束后，恢复按钮状态和文字
                    getCodeButton.setEnabled(true);
                    getCodeButton.setText("获取验证码");
                }
            }
        };

        // 首次启动倒计时
        handler.post(countdownRunnable);
    }
    // 弹出请求失败的对话框
    private void showRequestFailedDialog(String str) {
        runOnUiThread(new Runnable() {
            @Override
            public void run() {
                AlertDialog.Builder builder = new AlertDialog.Builder(RegisterActivity.this);
                builder.setTitle("注意")
                        .setMessage(str)
                        .setPositiveButton("确定", null)
                        .show();
            }
        });
    }
    private void setData(JSONObject responseJson) throws JSONException {
        // 提取键为"data"的值
        JSONObject dataJson = responseJson.getJSONObject("data");
        sharedPreferencesManager.setFigureShengao(dataJson.getString("shengao"));
        sharedPreferencesManager.setFigureTizhong(dataJson.getString("tizhong"));
        sharedPreferencesManager.setFigureTunwei(dataJson.getString("tunwei"));
        sharedPreferencesManager.setFigureXiongwei(dataJson.getString("xiongwei"));
        sharedPreferencesManager.setFigureYaowei(dataJson.getString("yaowei"));
        sharedPreferencesManager.setUserID(dataJson.getString("_id"));
        sharedPreferencesManager.setUserPassword(dataJson.getString("password"));
        sharedPreferencesManager.setUserPhone(dataJson.getString("phone_number"));
        sharedPreferencesManager.setUserRole(dataJson.getString("role"));
        sharedPreferencesManager.setUsername(dataJson.getString("username"));
        sharedPreferencesManager.setLoggedIn(true);
    }
}