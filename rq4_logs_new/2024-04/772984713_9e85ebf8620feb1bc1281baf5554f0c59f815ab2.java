package com.example.levents.Adapter;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.ViewGroup;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.example.levents.DAO.ThongKe_DAO;
import com.example.levents.Model.Khachhang;
import com.example.levents.databinding.ItemsTop3KhachhangBinding;
import com.squareup.picasso.Picasso;

import java.util.ArrayList;

public class Adapter_top3_khachhang extends RecyclerView.Adapter<Adapter_top3_khachhang.Viewh> {
    private ArrayList<Khachhang> list;
    private Context context;
    private ThongKe_DAO dao;
    public Adapter_top3_khachhang(ArrayList<Khachhang> list, Context context) {
        this.list = list;
        this.context = context;
        dao=new ThongKe_DAO(context);
    }
    @NonNull
    @Override
    public Adapter_top3_khachhang.Viewh onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        ItemsTop3KhachhangBinding binding=ItemsTop3KhachhangBinding.inflate(LayoutInflater.from(parent.getContext()),parent,false);
        return new Adapter_top3_khachhang.Viewh(binding);
    }

    @Override
    public void onBindViewHolder(@NonNull Adapter_top3_khachhang.Viewh holder, int position) {
        Picasso.get().load(list.get(position).getAnhkhachhang()).into(holder.biding.imgNguoidung);
        holder.biding.txtDHTennguoidung.setText("Tên khách hàng : " + String.valueOf(list.get(position).getHoten()));
        holder.biding.txtsoLuongDon.setText("Số đơn hàng : " + list.get(position).getSoluongdonhang());
    }

    @Override
    public int getItemCount() {
        return list.size();
    }
    public class Viewh extends RecyclerView.ViewHolder {
        ItemsTop3KhachhangBinding biding;
        public Viewh(@NonNull ItemsTop3KhachhangBinding biding) {
            super(biding.getRoot());
            this.biding=biding;
        }
    }
}