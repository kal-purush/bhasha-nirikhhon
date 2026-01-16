package com.example.mojing;

import androidx.appcompat.app.AppCompatActivity;

import android.app.AlertDialog;
import android.content.DialogInterface;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

public class SettingActivity extends AppCompatActivity {

    private Button exitBtn;
    private SharedPreferencesManager sharedPreferencesManager;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_setting);
        sharedPreferencesManager = new SharedPreferencesManager(this);
        exitBtn= findViewById(R.id.exitButton);
        exitBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                    showConfirmationDialog();
            }
        });
    }
    //确认退出登录弹窗
    private void showConfirmationDialog() {
        AlertDialog.Builder builder = new AlertDialog.Builder(this);
        builder.setTitle("确认退出登录");
        builder.setMessage("确定要退出登录吗？");
        builder.setPositiveButton("确定", new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int which) {
                // 执行注销操作
                DataClear();
//                new Thread(new Runnable() {
//                    @Override
//                    public void run() {
//                        MediaType JSON = MediaType.parse("application/json;charset=utf-8");
//                        JSONObject json = new JSONObject();
//                        try {
//                            json.put("phone_number", sharedPreferencesManager.getUserPhone());
//                        } catch (JSONException e) {
//                            throw new RuntimeException(e);
//                        }
//                        //创建一个OkHttpClient对象
//                        OkHttpClient okHttpClient = new OkHttpClient();
//                        RequestBody requestBody = RequestBody.create(JSON, String.valueOf(json));
//                        Request request = new Request.Builder()
//                                .url("http://47.102.43.156:8007/auth/remove-account")
//                                .post(requestBody)
//                                .build();
//                        // 发送请求并获取响应
//                        try {
//                            Response response = okHttpClient.newCall(request).execute();
//                            // 检查响应是否成功
//                            if (response.isSuccessful()) {
//                                // 获取响应体
//                                ResponseBody responseBody = response.body();
//                                // 处理响应数据
//                                String responseData = responseBody.string();
//                                System.out.println("Response: " + responseData);
//                                // 记得关闭响应体
//                                responseBody.close();
//                            } else {
//                                // 请求失败，处理错误
//                                System.out.println("Request failed");
//                            }
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }).start();
//                restartMainActivity();
                Intent intent = new Intent(SettingActivity.this, MainActivity.class);
                startActivity(intent);
                finish(); // 结束当前
            }
        });
        builder.setNegativeButton("取消", null);
        builder.show();
    }
    //退出登录
    private void DataClear()
    {
        sharedPreferencesManager.setLoggedIn(false);
        sharedPreferencesManager.setUsername("Username");
        sharedPreferencesManager.setUserPassword("user_password");
        sharedPreferencesManager.setFigureYaowei("yaowei");
        sharedPreferencesManager.setFigureXiongwei("xiongwei");
        sharedPreferencesManager.setFigureTizhong("tizhong");
        sharedPreferencesManager.setFigureTunwei("tunwei");
        sharedPreferencesManager.setFigureShengao("shengao");
        sharedPreferencesManager.setUserRole("userRole");
        sharedPreferencesManager.setUserID("userID");
        sharedPreferencesManager.setUserPhone("userPhone");
        //IsYouke重置
        sharedPreferencesManager.setIsYouke(false);
    }
}