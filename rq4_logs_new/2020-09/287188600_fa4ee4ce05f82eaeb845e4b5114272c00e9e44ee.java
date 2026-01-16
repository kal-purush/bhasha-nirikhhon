package com.example.woof.adapters;

import android.media.Image;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.example.woof.R;
import com.example.woof.database.productModel;
import com.example.woof.manage_accessory_single_item;

import java.util.ArrayList;

public class manageAccRVAdapter extends RecyclerView.Adapter<manageAccRVAdapter.RVViewHolderClass> {
    ArrayList<productModel> productModelList;

    public manageAccRVAdapter(ArrayList<productModel> productModelList) {
        this.productModelList = productModelList;
    }

    @NonNull
    @Override
    public RVViewHolderClass onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        return new RVViewHolderClass(LayoutInflater.from(parent.getContext())
                .inflate(R.layout.activity_manage_accessory_single_item,parent,false));
    }

    @Override
    public void onBindViewHolder(@NonNull RVViewHolderClass holder, int position) {
        productModel pm = productModelList.get(position);
        holder.prodName.setText(pm.getName());
        holder.proDesc.setText(pm.getDesc());
        String price = Float.toString(pm.getPrice());
        holder.prodPrice.setText(price);
        holder.prodName.setText(pm.getName());
        holder.prodImage.setImageBitmap(pm.getImage());
    }

    @Override
    public int getItemCount() {
        return productModelList.size();
    }

    public  static  class RVViewHolderClass extends RecyclerView.ViewHolder
    {
        TextView prodName,proDesc,prodPrice;
        ImageView prodImage;

        public RVViewHolderClass(@NonNull View itemView) {
            super(itemView);
            prodName = itemView.findViewById(R.id.itemName);
            proDesc = itemView.findViewById(R.id.prodDesc);
            prodPrice = itemView.findViewById(R.id.prodPrice);
            prodImage = itemView.findViewById(R.id.prodImage);

        }
    }
}