package com.example.duan1chinhthuc.Activity_user;

import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.ImageView;

import com.example.duan1chinhthuc.Adapter.adapter_donhang_user;
import com.example.duan1chinhthuc.Adapter.adapter_giohang;
import com.example.duan1chinhthuc.DAO.DAO_giohang;

import com.example.duan1chinhthuc.R;
import com.example.duan1chinhthuc.mode.gio_hang;

import java.util.ArrayList;

public class gio_hang_activity extends AppCompatActivity {
    RecyclerView rcv;
    DAO_giohang dao;
    adapter_giohang adapter;
    ArrayList<gio_hang> list;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_gio_hang);


        rcv = findViewById(R.id.rcv_cart);

        dao = new DAO_giohang(this);
        SharedPreferences sharedPreferences = getSharedPreferences("Thongtin_tt", Context.MODE_PRIVATE);
        String id = sharedPreferences.getString("id_usser","");
        list = dao.getDS_giohang_user(Integer.parseInt(id));
        Log.d("check", list.size()+"");
        adapter = new adapter_giohang(list, this);
        rcv.setAdapter(adapter);
        LinearLayoutManager layoutManager = new LinearLayoutManager(this);
        rcv.setLayoutManager(layoutManager);
        adapter.notifyDataSetChanged();

        ImageView back_12 = findViewById(R.id.back_122);
        back_12.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(gio_hang_activity.this, MainActivity.class);
                startActivity(intent);
            }
        });


    }
}