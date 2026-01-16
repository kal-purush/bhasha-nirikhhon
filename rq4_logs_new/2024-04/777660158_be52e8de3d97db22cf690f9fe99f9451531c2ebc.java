package com.example.pulopo.Activity;

import android.content.Intent;
import android.os.Bundle;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;

import com.example.pulopo.R;
import com.example.pulopo.Retrofit.ApiServer;
import com.example.pulopo.Retrofit.RetrofitClient;
import com.example.pulopo.Utils.UserUtil;
import com.example.pulopo.Utils.UtilsCommon;

import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;

public class UserInfoActivity extends AppCompatActivity {
    ApiServer apiServer;
    CompositeDisposable compositeDisposable = new CompositeDisposable();
    EditText userName, fullName, email, password, confirm;
    Button btn_update;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_user_info);
        apiServer = RetrofitClient.getInstance(UtilsCommon.BASE_URL).create(ApiServer.class);
        initView();

    }
    @Override
    public void onBackPressed() {
        super.onBackPressed(); // Gọi phương thức gốc để hoạt động mặc định của nút "Back"
        // Các xử lý khác nếu cần
        Intent intent = new Intent(UserInfoActivity.this,ConnectFriendsActivity.class);
        startActivity(intent);
    }
    private void initView() {
        userName = findViewById(R.id.input_info_name);
        fullName = findViewById(R.id.input_info_fullname);
        email = findViewById(R.id.input_info_email);
        password = findViewById(R.id.input_info_password);
        confirm = findViewById(R.id.input_info_confirm);

        userName.setText(UserUtil.getUserName());
        fullName.setText(UserUtil.getHoTen());
        email.setText(UserUtil.getEmail());

        btn_update = findViewById(R.id.btn_update);


        btn_update.setOnClickListener(view -> {
            String fullName_txt, email_txt, password_txt, confirm_txt;
            fullName_txt = String.valueOf(fullName.getText());
            email_txt = String.valueOf(email.getText());
            password_txt = String.valueOf(password.getText());
            confirm_txt = String.valueOf(confirm.getText());

            if (fullName_txt.equals("")||email_txt.equals("")) {
                Toast.makeText(getApplicationContext(), "Vui lòng nhập thông tin", Toast.LENGTH_SHORT).show();
            } else if (!password_txt.equals(confirm_txt)) {
                Toast.makeText(getApplicationContext(), "Vui lòng nhập mật khẩu và nhập lại giống nhau", Toast.LENGTH_SHORT).show();
            } else {
                compositeDisposable.add(apiServer.updateInfo(userName.getText().toString(), password_txt,fullName_txt, email_txt)
                        .subscribeOn(Schedulers.io())
                        .observeOn(AndroidSchedulers.mainThread())
                        .subscribe(
                                userModel -> {
                                    if (userModel.getSuccess()) {
                                        Toast.makeText(getApplicationContext(), "Cập nhật thông tin thành công", Toast.LENGTH_LONG).show();
                                        Intent intent = new Intent(UserInfoActivity.this,ConnectFriendsActivity.class);
                                        startActivity(intent);
                                    }
                                },
                                throwable -> {
                                    Toast.makeText(getApplicationContext(), throwable.getMessage(), Toast.LENGTH_LONG).show();
                                }
                        ));
            }
        });
    }
}