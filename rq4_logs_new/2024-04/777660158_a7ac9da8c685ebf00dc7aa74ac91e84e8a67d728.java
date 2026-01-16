package com.example.pulopo.Activity;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;

import android.Manifest;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.location.LocationManager;
import android.os.Build;
import android.os.Bundle;
import android.text.TextUtils;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import com.example.pulopo.R;
import com.example.pulopo.Services.Post_Location_Service;
import com.example.pulopo.Utils.UserUtil;
import com.example.pulopo.model.Users;
import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.Task;
import com.google.firebase.auth.AuthResult;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;

import java.util.ArrayList;

public class LoginActivity extends AppCompatActivity {
    Button btnLogin,btnRegister;
    EditText edtmail, edtPass;

    private FirebaseAuth firebaseAuth;
    FirebaseDatabase database;
    DatabaseReference myRef;
    ArrayList<Users> listUser = new ArrayList<>();

    private static final int request_Code = 12;
    LocationManager locationManager;




    private void createRequestPermission() {

        if(Build.VERSION.SDK_INT < Build.VERSION_CODES.M){
            return;
        }else{
            if(checkSelfPermission(Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED){
                return;
            }else {
                String [] permissionLog = {Manifest.permission.ACCESS_FINE_LOCATION};
                requestPermissions(permissionLog,request_Code);

            }
        }

    }
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode == request_Code) {
            if (grantResults.length > 0 && grantResults[0] == getPackageManager().PERMISSION_GRANTED) {
                Intent intent = new Intent(this, LoginActivity.class);
                startActivity(intent);
                Toast.makeText(this, "Truy cập địa chỉ đã bật", Toast.LENGTH_SHORT).show();
            } else {
                Toast.makeText(this, "Vui lòng bật truy cập địa chỉ", Toast.LENGTH_SHORT).show();
                finish();
            }
        }
    }



    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_login);
        init();
        createRequestPermission();
        notifyTurnOnLocation();
        insertInforUserLogin();

        btnRegister.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Intent intent = new Intent(LoginActivity.this,RegisterActivity.class);
                startActivity(intent);
            }
        });
        btnLogin.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
                    if(locationManager.isLocationEnabled()){
                        Login();
                    }else{
                        Toast.makeText(LoginActivity.this, "Bật định vị di động và khởi động lại ứng dụng", Toast.LENGTH_SHORT).show();

                    }
                }

            }
        });
    }

    private void insertInforUserLogin() {
        if(edtmail.getText().toString().isEmpty()){
            edtmail.setText(UserUtil.emailLogin);
            edtPass.setText(UserUtil.passwordLogin);

        }
    }

    private void notifyTurnOnLocation() {
        locationManager = (LocationManager) getSystemService(LOCATION_SERVICE);
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
            if(!locationManager.isLocationEnabled()){
                Toast.makeText(this, "Vui lòng bật định vị và khởi động lại ứng dụng", Toast.LENGTH_SHORT).show();
            Intent intent = new Intent(LoginActivity.this,Post_Location_Service.class);
            stopService(intent);
            finish();
            }
        }
    }

    private void getData() {
        String emailCheck = edtmail.getText().toString().trim();
        database = FirebaseDatabase.getInstance();
        myRef = database.getReference();
        myRef.child("My Family").addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(@NonNull DataSnapshot dataSnapshot) {

                for(DataSnapshot snapshot : dataSnapshot.getChildren()){
                    Users users = snapshot.getValue(Users.class);
                    listUser.add(users);
                }
                for(Users s: listUser){
                        if(s.getUID().equals(firebaseAuth.getCurrentUser().getUid())){
                        UserUtil.userName = s.getUserName().toString().trim();
                        UserUtil.UID = firebaseAuth.getCurrentUser().getUid();
                    }
                }
                listUser.clear();
            }

            @Override
            public void onCancelled(@NonNull DatabaseError error) {

            }
        });
    }

    private void Login() {
        if(UserUtil.userName.isEmpty()) {
            getData();
        }
                String email_tr, pass_str;
                email_tr = edtmail.getText().toString().trim();
                pass_str = edtPass.getText().toString().trim();
                if(TextUtils.isEmpty(email_tr) && TextUtils.isEmpty(pass_str)){
                    Toast.makeText(this, "Vui lòng nhập email, mật khẩu", Toast.LENGTH_SHORT).show();
                }else{
                    firebaseAuth.signInWithEmailAndPassword(email_tr,pass_str).addOnCompleteListener(new OnCompleteListener<AuthResult>() {
                        @Override
                        public void onComplete(@NonNull Task<AuthResult> task) {
                            if(task.isSuccessful()){
                                    UserUtil.statusLogin = true;
                                    createdSevices();
                                    Intent intent = new Intent(LoginActivity.this, MapsActivity.class);
                                    startActivity(intent);
                                    Toast.makeText(LoginActivity.this, "Đăng nhập thành công", Toast.LENGTH_SHORT).show();

                            }else {
                                    Toast.makeText(LoginActivity.this, "Đăng nhập thất bại, vui lòng kiểm tra lại email, mật khẩu", Toast.LENGTH_SHORT).show();
                            }
                        }
                    });
                }




    }

    private void createdSevices() {
        Intent intentSer = new Intent(this, Post_Location_Service.class);
        startService(intentSer);
    }

    public void init(){
        firebaseAuth = FirebaseAuth.getInstance();
        btnLogin = findViewById(R.id.btnLogin);
        btnRegister = findViewById(R.id.btnRegister);
        edtmail = findViewById(R.id.edtMail);
        edtPass = findViewById(R.id.inputTextPassword);
    }
}