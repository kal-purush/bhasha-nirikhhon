package com.example.duan1chinhthuc.Adapter;

import android.app.Activity;
import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.example.duan1chinhthuc.DAO.Home_DAO;
import com.example.duan1chinhthuc.R;
import com.example.duan1chinhthuc.mode.gio_hang;

import java.util.ArrayList;
import java.util.List;

public class ProductAdapter_cart extends RecyclerView.Adapter<ProductAdapter_cart.viewholder> {
    private final ArrayList<gio_hang> list;
    private final Context context;

    ProductAdapter_cart adapter;
    Home_DAO dao;

    public ProductAdapter_cart(ArrayList<gio_hang> list, Context context) {
        this.list =  list;
        this.context = context;
    }

    @NonNull
    @Override
    public ProductAdapter_cart.viewholder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        LayoutInflater inflater = ((Activity) context).getLayoutInflater();
        View view =inflater.inflate(R.layout.item_sp_giohang,parent,false);

        return new ProductAdapter_cart.viewholder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull ProductAdapter_cart.viewholder holder, int position) {
        holder.gia.setText(Integer.toString(list.get(position).getGiasp()));
        holder.id.setText(Integer.toString(list.get(position).getId()));
        holder.tenSP.setText(list.get(position).getTensp());



        adapter = new ProductAdapter_cart(list, context);
        dao = new Home_DAO(context);
    }

    @Override
    public int getItemCount() {
        return list.size();
    }

    public class viewholder extends RecyclerView.ViewHolder {
//        private int id;
//        private String tensp;
//        private int giasp;
//        private int hinhanhsp;
//        private int soluongsp;
        TextView tenSP,gia,id,sl;
        public viewholder(@NonNull View itemView) {
            super(itemView);
            tenSP = itemView.findViewById(R.id.ten_sp_giohang);
            gia = itemView.findViewById(R.id.giatien_sp_giohang);

            id = itemView.findViewById(R.id.id_sp_giohang);
        }
    }
}