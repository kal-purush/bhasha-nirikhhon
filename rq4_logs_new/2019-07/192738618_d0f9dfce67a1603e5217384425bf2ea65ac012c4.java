package com.erzihutama.liburanyuk.adapter;

import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.support.v7.widget.CardView;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.TextView;
import android.widget.Toast;

import com.bumptech.glide.Glide;
import com.erzihutama.liburanyuk.R;
import com.erzihutama.liburanyuk.model.CustomOnItemClickListener;

import com.erzihutama.liburanyuk.model.OutdorModel;
import com.erzihutama.liburanyuk.model.OutdorResponse;
import com.erzihutama.liburanyuk.view.home.DetailActivity;


import java.util.ArrayList;
import java.util.List;

import retrofit2.Callback;

public class CardviewAdapter extends RecyclerView.Adapter<CardviewAdapter.CardViewHolder> {

    private ArrayList<OutdorModel> listOutdorModels;
    private Context context;
    private Activity activity;

    public CardviewAdapter(Context context) {
        this.context = context;
    }

    public CardviewAdapter(Callback<OutdorResponse> artactivity, List<OutdorModel> list) {
    }

    public ArrayList<OutdorModel> getListOutdorModels() {
        return listOutdorModels;
    }

    public void setListOutdorModels(ArrayList<OutdorModel> listOutdorModels) {
        this.listOutdorModels = listOutdorModels;
    }

    @Override
    public CardViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.item_cardview, parent, false);
        CardViewHolder viewHolder = new CardViewHolder(view);
        return viewHolder;
    }

    @Override
    public void onBindViewHolder(CardViewHolder holder, int position) {
        final OutdorModel k = getListOutdorModels().get(position);
        Glide.with(context)
                .load(k.getPhoto())
                .override(350,550)
                .into(holder.imgPhoto);
        holder.tvname.setText(k.getNama_tempat());
        holder.tvremarks.setText(k.getDaerah());
        holder.outdorModel = k;




        holder.btnsahre.setOnClickListener(new CustomOnItemClickListener(position, new CustomOnItemClickListener.OnItemClickCallback() {
            @Override
            public void onItemClicked(View view, int position) {
                Toast.makeText(context, "Share"+ getListOutdorModels().get(position).getNama_tempat(), Toast.LENGTH_SHORT).show();
            }
        }));


    }

    @Override
    public int getItemCount() {
        return getListOutdorModels().size();
    }

    public void startactivity(Intent intent){

    }

    public class CardViewHolder extends RecyclerView.ViewHolder implements View.OnClickListener {
         ImageView imgPhoto;
         TextView tvname, tvremarks;
         Button btndetail, btnsahre;
         CardView cardView;
         OutdorModel outdorModel;

        public CardViewHolder(View itemView) {
            super(itemView);
            imgPhoto = (ImageView)itemView.findViewById(R.id.img_item_photo);
            tvname = (TextView)itemView.findViewById(R.id.tv_item_name);
            tvremarks = (TextView)itemView.findViewById(R.id.tv_item_remarks);
            btnsahre = (Button)itemView.findViewById(R.id.btn_set_share);
            btndetail = (Button)itemView.findViewById(R.id.btn_set_detail);
            cardView = (CardView)itemView.findViewById(R.id.item_card);
            cardView.setOnClickListener(this);
        }

        @Override
        public void onClick(View view) {
            Intent intent = new Intent(context, DetailActivity.class);
            intent.putExtra("key", outdorModel);
            context.startActivity(intent);
        }
    }
}