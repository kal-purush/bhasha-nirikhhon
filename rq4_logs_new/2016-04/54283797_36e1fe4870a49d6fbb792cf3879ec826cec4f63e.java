package com.bankfinder.chathurangasandun.bankfinder;

import android.content.Context;
import android.media.Image;
import android.support.v7.widget.RecyclerView;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import com.bankfinder.chathurangasandun.bankfinder.model.Branches;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by Chathuranga Sandun on 4/22/2016.
 */
public class BranchAdapter extends RecyclerView.Adapter<BranchAdapter.BranchViewholder>{


    private List<Branches> branchesList;

    public class BranchViewholder extends RecyclerView.ViewHolder {
        public TextView tvName,tvAddress;
        public ImageView imgIcon;

        public BranchViewholder(View view) {
            super(view);

            tvName = (TextView) view.findViewById(R.id.tvName);
            tvAddress= (TextView) view.findViewById(R.id.tvAddress);
            imgIcon = (ImageView) view.findViewById(R.id.imgicon);

        }
    }


    public BranchAdapter(List<Branches> branchesList) {
        this.branchesList = branchesList;
    }

    @Override
    public BranchViewholder onCreateViewHolder(ViewGroup parent, int viewType) {
        View itemView = LayoutInflater.from(parent.getContext())
                .inflate(R.layout.branch_row, parent, false);

        return new BranchViewholder(itemView);
    }

    @Override
    public void onBindViewHolder(BranchViewholder holder, int position) {
        Branches branch = branchesList.get(position);
        holder.tvName.setText(branch.getName());
        holder.tvAddress.setText(branch.getAddress());
    }

    @Override
    public int getItemCount() {
        return branchesList.size();
    }


}


