package com.example.mr_shareone.permission;

import android.support.annotation.NonNull;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.widget.Toast;

import java.util.List;

import pub.devrel.easypermissions.EasyPermissions;

public class TestEasyPermission extends AppCompatActivity implements EasyPermissions.PermissionCallbacks{

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_test_easy_permission);

        if(EasyPermissions.hasPermissions(this,"android.permission.ACCESS_FINE_LOCATION")){
            Toast.makeText(this,"已经有android.permission.ACCESS_FINE_LOCATION权限",Toast.LENGTH_LONG).show();
        }else{
            EasyPermissions.requestPermissions(this,"",1,"android.permission.ACCESS_FINE_LOCATION");
            Toast.makeText(this,"没有android.permission.ACCESS_FINE_LOCATION权限",Toast.LENGTH_LONG).show();
        }
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        EasyPermissions.onRequestPermissionsResult(requestCode,permissions,grantResults);
    }

    @Override
    public void onPermissionsGranted(int requestCode, @NonNull List<String> perms) {
        Toast.makeText(this,perms.toString()+"已经获取到权限",Toast.LENGTH_LONG).show();
    }

    @Override
    public void onPermissionsDenied(int requestCode, @NonNull List<String> perms) {
        Toast.makeText(this,perms.toString()+"未被赋予权限",Toast.LENGTH_LONG).show();
    }
}