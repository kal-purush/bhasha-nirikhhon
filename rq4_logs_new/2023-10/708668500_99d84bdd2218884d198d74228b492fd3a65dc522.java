package com.jz.util;

import android.content.ClipData;
import android.content.Context;
import android.text.TextUtils;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.recyclerview.widget.RecyclerView;

import java.util.List;

public class BlueAdapter extends RecyclerView.Adapter<BlueAdapter.ViewHolder> {

    private Context mContext;
    private List<ItemBean> lists;

    public BlueAdapter(List<ItemBean> list,Context context){
        this.lists = list;
        this.mContext = context;
    }

    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        return new ViewHolder(LayoutInflater.from(mContext).inflate(R.layout.item,null));
    }

    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, int position) {
            ItemBean item = lists.get(position);
            if(TextUtils.equals(item.getName(),"null")){
                item.setName("未知");
            }
            holder.name.setText(item.getName());
            holder.mac.setText(position+"、"+item.getMac());
    }

    @Override
    public int getItemCount() {
        return lists == null ? 0 : lists.size();
    }


    class ViewHolder extends RecyclerView.ViewHolder {
        TextView name;
        TextView mac;

        public ViewHolder(@NonNull View itemView) {
            super(itemView);
            name = itemView.findViewById(R.id.tv_name);
            mac = itemView.findViewById(R.id.tv_mac);
        }
    }

}








