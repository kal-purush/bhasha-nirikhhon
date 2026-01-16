package com.example.levents.Activity;

import android.annotation.SuppressLint;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.util.Log;
import android.view.MenuItem;

import androidx.annotation.NonNull;
import androidx.appcompat.app.ActionBarDrawerToggle;
import androidx.appcompat.app.AppCompatActivity;
import androidx.appcompat.widget.Toolbar;
import androidx.drawerlayout.widget.DrawerLayout;
import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentManager;
import androidx.fragment.app.FragmentTransaction;

import com.example.levents.Fragment.QLSpham_Fragment;
import com.example.levents.Fragment.QL_nguoidung_Fragment;
import com.example.levents.Fragment.QL_nhanvien_Fragment;
import com.example.levents.Fragment.QLhoadon_Fragment;
import com.example.levents.Fragment.Thongke_Fragment;
import com.example.levents.R;
import com.google.android.material.navigation.NavigationView;

public class Admin_Activity extends AppCompatActivity {
    DrawerLayout drawerLayout;
    Toolbar toolbar;
    NavigationView navigationView;


    @SuppressLint("MissingInflatedId")
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_admin);
        drawerLayout = findViewById(R.id.drawerLayout);
        toolbar = findViewById(R.id.toolbar);
        navigationView = findViewById(R.id.navigationView);
        setSupportActionBar(toolbar);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);

        ActionBarDrawerToggle drawerToggle = new ActionBarDrawerToggle(
                Admin_Activity.this,
                drawerLayout,
                toolbar,
                R.string.open,
                R.string.close
        );
        drawerToggle.setDrawerIndicatorEnabled(true);
        drawerToggle.syncState();
        drawerLayout.addDrawerListener(drawerToggle);

        navigationView.setNavigationItemSelectedListener(new NavigationView.OnNavigationItemSelectedListener() {
            @Override
            public boolean onNavigationItemSelected(@NonNull MenuItem menuItem) {
                Fragment fragment = null;
                if (menuItem.getItemId() == R.id.sanPham) {
                    toolbar.setTitle("Sản phẩm");
                    fragment = new QLSpham_Fragment();
                } else if (menuItem.getItemId() == R.id.khachHang) {
                    toolbar.setTitle("Khách hàng");
                    fragment = new QL_nguoidung_Fragment();
                }  else if(menuItem.getItemId() == R.id.qlhoadon){
                    toolbar.setTitle("Quản lý hoá đơn");
                    fragment = new QLhoadon_Fragment();
                } else if(menuItem.getItemId() == R.id.qlnhanvien){
                    toolbar.setTitle("Quản lý nhân viên");
                    fragment = new QL_nhanvien_Fragment();
                } else if(menuItem.getItemId() == R.id.qlthongke){
                    toolbar.setTitle("Thống kê doanh thu");
                    fragment = new Thongke_Fragment();
                }
                else if (menuItem.getItemId() == R.id.dangXuat) {
                    finish();
                    startActivity(new Intent(Admin_Activity.this, Login_Activity.class));
                    return true;
                }
                getSupportFragmentManager().beginTransaction().replace(R.id.frameLayout, fragment).commit();
                drawerLayout.close();
                if (fragment != null) {
                    getSupportFragmentManager().beginTransaction().replace(R.id.frameLayout, fragment).commit();
                    drawerLayout.close();
                    return true;
                }
                return false;

            }
        });
        Loadfragment(new QLSpham_Fragment(),true);
        SharedPreferences sharedPreferences = getSharedPreferences("ADMIN", MODE_PRIVATE);
        String loaiTaiKhoan = sharedPreferences.getString("loaitaikhoan", "");
        Log.d("check","taikhoan" + loaiTaiKhoan);
    }

    private void Loadfragment(Fragment fragment, boolean isAppInitialized){
        FragmentManager fragmentManager = getSupportFragmentManager();
        FragmentTransaction fragmentTransaction = fragmentManager.beginTransaction();
        if (isAppInitialized){
            fragmentTransaction.add(R.id.frameLayout,fragment);
        } else {
            fragmentTransaction.replace(R.id.frameLayout, fragment);
        }

        fragmentTransaction.commit();
    }
}