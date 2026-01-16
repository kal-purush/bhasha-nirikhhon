package com.example.travelappfragment;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import java.util.ArrayList;

public class HomePlaceAdapter extends RecyclerView.Adapter<HomePlaceAdapter.MyViewHolder>{
    Context context;
    ArrayList<Places> placesArrayList;

    public HomePlaceAdapter(Context context, ArrayList<Places> placesArrayList){
        this.context = context;
        this.placesArrayList = placesArrayList;

    }


    @NonNull
    @Override
    public MyViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View v = LayoutInflater.from(context).inflate(R.layout.item_places, parent, false);

        return new MyViewHolder(v);
    }

    @Override
    public void onBindViewHolder(@NonNull MyViewHolder holder, int position) {
        Places places = placesArrayList.get(position);
        holder.txt_placeName.setText(places.placeName);
        holder.txt_placeDes.setText(places.placeDes);
        holder.imgPlace.setImageResource(places.placeImg);
    }

    @Override
    public int getItemCount() {
        return placesArrayList.size();

    }

    public static class MyViewHolder extends RecyclerView.ViewHolder{

        TextView txt_placeName, txt_placeDes;

        ImageView imgPlace;

        public MyViewHolder(@NonNull View itemView) {

            super(itemView);

            txt_placeName = itemView.findViewById(R.id.txtPlaceName);
            txt_placeDes = itemView.findViewById(R.id.txtPlaceDesciption);
            imgPlace = itemView.findViewById(R.id.imgPlace);


        }
    }
}