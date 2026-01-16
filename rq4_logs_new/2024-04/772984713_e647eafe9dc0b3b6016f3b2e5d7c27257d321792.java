package com.example.levents.Adapter;

import android.annotation.SuppressLint;
import android.app.AlertDialog;
import android.content.Context;
import android.content.DialogInterface;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.example.levents.DAO.Nhanvien_DAO;
import com.example.levents.Database.DBhelper;
import com.example.levents.Model.Khachhang;
import com.example.levents.Model.Nhanvien;
import com.example.levents.R;
import com.squareup.picasso.Picasso;

import java.util.ArrayList;

public class Nhanvien_Adapter extends RecyclerView.Adapter<Nhanvien_Adapter.ViewHolder> {
    Context context;
    Nhanvien_DAO nhanvienDao;
    DBhelper dBhelper;
    ArrayList<Nhanvien> listNV;

    public Nhanvien_Adapter(Context context, Nhanvien_DAO nhanvienDao, ArrayList<Nhanvien> listNv) {
        this.context = context;
        this.nhanvienDao = nhanvienDao;
        this.listNV = listNv;
        notifyDataSetChanged();
    }
    @NonNull
    @Override
    public Nhanvien_Adapter.ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(context).inflate(R.layout.item_qlnguoidung, parent, false);
        return new Nhanvien_Adapter.ViewHolder(view);
    }

    @SuppressLint("RecyclerView")
    @Override
    public void onBindViewHolder(@NonNull Nhanvien_Adapter.ViewHolder holder, int position) {
        Nhanvien nhanvien = listNV.get(position);
        holder.manguoidung.setText(String.valueOf(nhanvien.getManhanvien()));
        holder.hovaten.setText(nhanvien.getHoten());
        holder.email.setText(nhanvien.getEmail());
        holder.diachi.setText(nhanvien.getDiachi());
        holder.sdt.setText(nhanvien.getSodienthoai());
        Picasso.get().load(nhanvien.getAnhnhanvien()).into(holder.imageND);
        holder.xoaND.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                showDeleteConfirmationDialog(position);
            }
        });

    }
    private void showDeleteConfirmationDialog(final int position) {
        AlertDialog.Builder builder = new AlertDialog.Builder(context);
        builder.setMessage("Bạn có chắc muốn xóa người dùng này?")
                .setPositiveButton("Xóa", new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int id) {

                        listNV.remove(position);
                        notifyDataSetChanged();
                    }
                })
                .setNegativeButton("Hủy", new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int id) {

                        dialog.dismiss();
                    }
                });
        // Hiển thị dialog
        builder.create().show();
    }

    @Override
    public int getItemCount() {
        return listNV.size();
    }

    public class ViewHolder extends RecyclerView.ViewHolder{
        ImageView imageND, xoaND;
        TextView manguoidung, hovaten, email, diachi, sdt, tien;

        public ViewHolder (@NonNull View itemView) {
            super(itemView);
            xoaND = itemView.findViewById(R.id.img_xoaND);
            imageND = itemView.findViewById(R.id.img_nguoidung);
            manguoidung = itemView.findViewById(R.id.tv_manguoidung);
            hovaten = itemView.findViewById(R.id.tv_tennguoidung);
            email = itemView.findViewById(R.id.tv_emailnguoidung);
            diachi = itemView.findViewById(R.id.tv_diachinguoidung);
            sdt = itemView.findViewById(R.id.tv_sdtnguoidung);
            tien = itemView.findViewById(R.id.tv_tiennguoidung);
        }
    }
}