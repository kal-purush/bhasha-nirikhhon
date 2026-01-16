package com.example.duan1chinhthuc.Adapter;

import android.annotation.SuppressLint;
import android.app.Activity;
import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Spinner;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.example.duan1chinhthuc.DAO.DonHang_DAO;
import com.example.duan1chinhthuc.DAO.Home_DAO;
import com.example.duan1chinhthuc.InterfaceRecycle;
import com.example.duan1chinhthuc.R;
import com.example.duan1chinhthuc.mode.Donhang;
import com.example.duan1chinhthuc.mode.San_Pham;

import java.util.ArrayList;

public class adapter_danhmuc extends RecyclerView.Adapter<adapter_danhmuc.viewholder>{

    private final Context context;
    ArrayList<San_Pham> list = new ArrayList<>();

    adapter_danhmuc adapter;
    Home_DAO dao;


    ArrayList<Donhang> list12;
    private static int nextId = 1;
    public adapter_danhmuc(ArrayList<San_Pham> list, Context context) {
        this.list = list;
        this.context = context;
    }
    @NonNull
    @Override
    public adapter_danhmuc.viewholder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        LayoutInflater inflater = ((Activity) context).getLayoutInflater();
        View view =inflater.inflate(R.layout.item_sp_danhmuc,parent,false);

        return new adapter_danhmuc.viewholder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull adapter_danhmuc.viewholder holder, @SuppressLint("RecyclerView") int position) {
        holder.tenSP.setText(list.get(position).getTensp());
        holder.giatien.setText(Integer.toString(list.get(position).getGiatien()));
        holder.ten_loaisp.setText(list.get(position).getTen_loaisp());

        adapter = new adapter_danhmuc(list, context);
        dao = new Home_DAO(context);
    }

    @Override
    public int getItemCount() {
        return list.size();
    }

    public class viewholder extends RecyclerView.ViewHolder {
        TextView idSP,tenSP,tieude,ngayban,trangthai,id_CL,ten_CL,id_sp_CT,giatien,size,id_loaisp,ten_loaisp,user_loai_sp1;
        public viewholder(@NonNull View itemView) {
            super(itemView);

            tenSP = itemView.findViewById(R.id.ten_sp_danhmuc);

            giatien = itemView.findViewById(R.id.gia_sp_danhmuc);

            ten_loaisp = itemView.findViewById(R.id.loai_sp_danhmuc);

        }
    }
}