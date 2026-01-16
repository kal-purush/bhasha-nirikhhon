package com.example.trekkin.ui.community.fragments;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.example.trekkin.R;

public class ProgrammingAdapterFriends extends RecyclerView.Adapter<ProgrammingAdapterFriends.ProgramminViewHolderFriends>{

    private String[] data;
    public ProgrammingAdapterFriends(String[] data){
        this.data = data;
    }

    @NonNull
    @Override
    public ProgramminViewHolderFriends onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        LayoutInflater inflater = LayoutInflater.from(parent.getContext());
        View view = inflater.inflate(R.layout.list_item_layout_friends, parent, false);
        return new ProgramminViewHolderFriends(view);
    }

    @Override
    public void onBindViewHolder(@NonNull ProgramminViewHolderFriends holder, int position) {
        String title = data[position];
        holder.mPersonName.setText(title);
    }

    @Override
    public int getItemCount() {
        return data.length;
    }

    public class ProgramminViewHolderFriends extends RecyclerView.ViewHolder{
        ImageView mProfileIcon;
        TextView mPersonName;
        public ProgramminViewHolderFriends(@NonNull View itemView) {
            super(itemView);
            mProfileIcon = itemView.findViewById(R.id.profile_icon);
            mPersonName = itemView.findViewById(R.id.person_name);
        }
    }
}