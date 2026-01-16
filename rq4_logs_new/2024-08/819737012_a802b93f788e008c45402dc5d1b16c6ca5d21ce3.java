package com.example.duan1_catmusic.Adapter;

import android.content.Context;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.example.duan1_catmusic.R;
import com.example.duan1_catmusic.model.Nhac;

import java.util.ArrayList;
import java.util.List;

public class BaiHatyeuThichAdapter extends RecyclerView.Adapter<BaiHatyeuThichAdapter.ViewHolder>{

    private java.util.List<Nhac> list;
    private Context context;

    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        return null;
    }

    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, int position) {

    }

    @Override
    public int getItemCount() {
        return 0;
    }

    public static class ViewHolder extends RecyclerView.ViewHolder {
        ImageView imgNhac, imLove;
        TextView tvTenNhac;
        TextView tvNgheSi;

        public ViewHolder(@NonNull View itemView) {
            super(itemView);
            imgNhac = itemView.findViewById(R.id.img_item_listnhac);
            tvTenNhac = itemView.findViewById(R.id.tvTenNhac);
            tvNgheSi = itemView.findViewById(R.id.tvNgheSi);
            imLove = itemView.findViewById(R.id.imgLove);




        }

    }
}