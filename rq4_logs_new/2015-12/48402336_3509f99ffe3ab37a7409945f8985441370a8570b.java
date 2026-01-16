package com.client.client;

import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.preference.PreferenceManager;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.widget.Button;
import android.widget.Checkable;
import android.widget.EditText;
import android.widget.Toast;

/**
 * Created by Administrator on 2015/12/30.
 */
public class SignInActivity extends AppCompatActivity {
    private SharedPreferences pref;
    private SharedPreferences.Editor editor;
    private EditText accountEdit;
    private EditText passwordEdit;
    private Button login;
    private Checkable rememberPass;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.signin);

        pref = PreferenceManager.getDefaultSharedPreferences(this);
        accountEdit = (EditText) findViewById(R.id.loginusername);
        passwordEdit = (EditText) findViewById(R.id.loginpassword);
        login = (Button) findViewById(R.id.loginbutton);
        rememberPass = (Checkable) findViewById(R.id.remember_pass);
        boolean isRemeber = pref.getBoolean("remember_password", false);
        if (isRemeber) {
            //将账号和密码都设置到文本框中
            String loginusername = pref.getString("loginusername", "");
            String loginpassword = pref.getString("loginpassword", "");
            accountEdit.setText(loginusername);
            passwordEdit.setText(loginpassword);
            rememberPass.isChecked();
        }
        login.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String loginusername = accountEdit.getText().toString();
                String loginpassword = passwordEdit.getText().toString();
                if (loginusername.equals("admin") && loginpassword.equals("123456")) {
                    editor = pref.edit();
                    if (rememberPass.isChecked()) {   //检查复选框是否被选中
                        editor.putBoolean("remember_password", true);
                        editor.putString("loginusername", loginusername);
                        editor.putString("loginpassword", loginpassword);
                    }else{
                        editor.clear();
                    }
                    editor.commit();
                    Intent intent = new Intent(SignInActivity.this,MainActivity.class);
                    startActivity(intent);
                    finish();
                }else{
                    Toast.makeText(SignInActivity.this,"账号或者密码错误",Toast.LENGTH_SHORT).show();
                }
            }
        });
    }
}