package com.giovas.adapters;

import android.content.Context;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import com.giovas.weatherapp.R;

import java.util.ArrayList;

/**
 * Created by DarkGeat on 10/28/2015.
 */
public class ZipCodeAdapter extends RecyclerView.Adapter<ZipCodeAdapter.ViewHolder> {

    private ArrayList<String> zipcodes;
    private Context context;

    public ZipCodeAdapter(Context c, ArrayList<String> list){
        zipcodes = list;
        context = c;
    }

    @Override
    public ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(context).inflate(R.layout.layout_item_zipcodes,parent,false);
        ViewHolder viewHolder = new ViewHolder(view);
        return viewHolder;
    }

    @Override
    public void onBindViewHolder(ViewHolder holder, int position) {
        holder.zipcode.setText(zipcodes.get(position));
    }

    @Override
    public int getItemCount() {
        return zipcodes.size();
    }

    public class ViewHolder extends RecyclerView.ViewHolder{

        private final TextView zipcode;
        private final ImageView removeZipcode;

        public ViewHolder(View itemView) {
            super(itemView);
            zipcode = (TextView)itemView.findViewById(R.id.textZipCode);
            removeZipcode = (ImageView)itemView.findViewById(R.id.removeZipCode);
        }
    }
}