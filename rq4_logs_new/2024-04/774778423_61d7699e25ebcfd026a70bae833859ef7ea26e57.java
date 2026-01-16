package com.example.duan1chinhthuc.Frament_user;

import android.annotation.SuppressLint;
import android.os.Bundle;

import androidx.fragment.app.Fragment;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;
import androidx.viewpager.widget.ViewPager;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import com.example.duan1chinhthuc.Adapter.SlideAdapter;
import com.example.duan1chinhthuc.Adapter.adapter_user;
import com.example.duan1chinhthuc.DAO.Home_DAO;
import com.example.duan1chinhthuc.R;
import com.example.duan1chinhthuc.mode.SanPhamChiTiet;
import com.example.duan1chinhthuc.mode.San_Pham;
import com.example.duan1chinhthuc.mode.gio_hang;

import java.util.ArrayList;


public class best_seller extends Fragment {
    private static boolean visible;
    RecyclerView rcv;
    Home_DAO dao;
    adapter_user adapter;

    ArrayList<San_Pham> list;

    ArrayList<SanPhamChiTiet> list1;
    public static ArrayList<gio_hang> manggiohang;
    ViewPager viewPager;
    @SuppressLint("MissingInflatedId")
    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        // Inflate the layout for this fragment
        View view = inflater.inflate(R.layout.fragment_best_seller, container, false);

        rcv = view.findViewById(R.id.fragment_bestseller_rcv);
        LinearLayoutManager layoutManager = new LinearLayoutManager(getContext());
        rcv.setLayoutManager(layoutManager);

        dao = new Home_DAO(getContext());
        list = dao.getDS_sanpham();
        adapter = new adapter_user(list, getContext());
        rcv.setAdapter(adapter);

        return view;
    }
}