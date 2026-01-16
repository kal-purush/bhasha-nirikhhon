package com.example.levents.Fragment;

import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentManager;
import androidx.fragment.app.FragmentTransaction;
import androidx.recyclerview.widget.LinearLayoutManager;

import com.example.levents.Adapter.Hoadon_Adapter;
import com.example.levents.DAO.Chitiethoadon_DAO;
import com.example.levents.DAO.Donhang_DAO;
import com.example.levents.Interface.OnItemClick;
import com.example.levents.Model.Hoadon;
import com.example.levents.R;

import java.util.ArrayList;


public class DS_hoadon_Fragment extends Fragment {
    com.example.levents.databinding.FragmentQLhoadonBinding binding;
    private ArrayList<Hoadon> list = new ArrayList<>();
    private Donhang_DAO donhangDao;
    private Hoadon_Adapter hoadonAdapter;
    Chitiethoadon_DAO chitiethoadonDao;
    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        binding = com.example.levents.databinding.FragmentQLhoadonBinding.inflate(inflater, container, false);
        donhangDao = new Donhang_DAO(getContext());
        LinearLayoutManager layoutManager = new LinearLayoutManager(getContext());
        String trangthai = "Đã Đặt Hàng";
        binding.rcvHoadon.setLayoutManager(layoutManager);
        list = donhangDao.getDsDonHangTheoTrangThai(trangthai);
        hoadonAdapter = new Hoadon_Adapter(list,getContext());
        binding.rcvHoadon.setAdapter(hoadonAdapter);
        hoadonAdapter.setOnItemClick(new OnItemClick() {
            @Override
            public void onItemClick(int position) {
                Hoadon donHang = list.get(position);
                int maDonHang = donHang.getMaDonHang();
                Bundle bundle = new Bundle();
                bundle.putInt("mahoadon", maDonHang);
                Hoadonchitiet_Fragment hoadonchitietFragment = new Hoadonchitiet_Fragment();
                hoadonchitietFragment.setArguments(bundle);
                FragmentManager fragmentManager = getParentFragmentManager();
                FragmentTransaction fragmentTransaction = fragmentManager.beginTransaction();
                fragmentTransaction.setCustomAnimations(R.anim.slide_in_right, R.anim.slide_out_left, R.anim.slide_in_left, R.anim.slide_out_right);
                fragmentTransaction.replace(R.id.frameLayout, hoadonchitietFragment );
                fragmentTransaction.addToBackStack(null);
                fragmentTransaction.commit();
            }
        });
        return binding.getRoot();
    }
}