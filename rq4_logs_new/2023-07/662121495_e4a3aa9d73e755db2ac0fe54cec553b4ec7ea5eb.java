package com.example.mojing.Adapter;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.example.mojing.R;

import java.util.List;

public class VipListAdapter extends RecyclerView.Adapter<VipListAdapter.MyViewHolder> {

    private List<String> data;

    public VipListAdapter(List<String> data) {
        this.data = data;
    }

    public static class MyViewHolder extends RecyclerView.ViewHolder {
        TextView textView;

        public MyViewHolder(View view) {
            super(view);
            textView = view.findViewById(R.id.textView1);
        }
    }

    @NonNull
    @Override
    public MyViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext())
                .inflate(R.layout.recycler_view_item, parent, false);
        return new MyViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull MyViewHolder holder, int position) {
        String item = data.get(position);
        holder.textView.setText(item);
    }

    @Override
    public int getItemCount() {
        return data.size();
    }
}