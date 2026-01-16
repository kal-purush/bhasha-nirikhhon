package com.example.franciscoandrade.retrofitbasics;

import android.content.Context;
import android.support.v7.widget.RecyclerView;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;

import com.squareup.picasso.Picasso;

import java.util.ArrayList;

/**
 * Created by franciscoandrade on 12/17/17.
 */

public class DogsAdapter extends RecyclerView.Adapter<DogsAdapter.DogsViewHolder> {
    ArrayList<DogsModel> lista;
    Context context;

    public DogsAdapter(ArrayList<DogsModel> lista, Context context) {
        this.lista = lista;
        this.context=context;
    }

    @Override
    public DogsAdapter.DogsViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {

        View view= LayoutInflater.from(parent.getContext()).inflate(R.layout.item_view, parent, false);

        return new DogsViewHolder(view);
    }

    @Override
    public void onBindViewHolder(DogsAdapter.DogsViewHolder holder, int position) {
        String url= lista.get(position).getImageUrl().toString();

        Log.d("ADAPTER", "onBindViewHolder: "+url);
        Picasso.with(context)
                .load(url)
                .fit()
                .into(holder.imageContainer);

    }

    @Override
    public int getItemCount() {
        return lista.size();
    }

    public class DogsViewHolder extends RecyclerView.ViewHolder {

        ImageView imageContainer;

        public DogsViewHolder(View itemView) {
            super(itemView);
            imageContainer=(ImageView)itemView.findViewById(R.id.imageContainer);

        }
    }
}