package com.varshney.deliverysystem.Adapters;

import android.content.Context;
import android.support.v7.widget.RecyclerView;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import com.varshney.deliverysystem.Pojos.Fashion;
import com.varshney.deliverysystem.R;

import java.util.ArrayList;

/**
 * Created by yash on 23/3/18.
 */

public class FashionAdapter extends RecyclerView.Adapter<FashionAdapter.ViewHolder> {

    public static final String TAG = "FA";
    Context context;
    ArrayList<Fashion> fashionArrayList = new ArrayList<>();

    public FashionAdapter(Context context, ArrayList<Fashion> fashionArrayList) {
        this.context = context;
        this.fashionArrayList = fashionArrayList;
    }

    @Override
    public ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        LayoutInflater li = (LayoutInflater) context.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
        View itemView = li.inflate(R.layout.fragment_fashion,parent,false);
        return new ViewHolder(itemView);
    }

    @Override
    public void onBindViewHolder(ViewHolder holder, int position) {
        Log.d(TAG, "onBindViewHolder: "+position);
         Fashion thisItem = fashionArrayList.get(position);
        // holder.tvCount.setText(thisItem.getCount_of_item());
         holder.tvName.setText(thisItem.getName());

    }

    @Override
    public int getItemCount() {
        return fashionArrayList.size();
    }

    public static class ViewHolder extends RecyclerView.ViewHolder{

        //TextView tvCount;
        TextView tvName;
        ImageView ivFavourite;

        public ViewHolder(View itemView) {
            super(itemView);
           // tvCount = itemView.findViewById(R.id.tvCount);
            tvName = itemView.findViewById(R.id.tvCardTitle);
        }
    }
}