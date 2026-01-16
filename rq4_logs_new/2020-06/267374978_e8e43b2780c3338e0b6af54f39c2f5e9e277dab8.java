package com.example.taxio;

import android.content.Context;
import android.content.Intent;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import java.util.ArrayList;

public class RecyclerTripSchedule extends RecyclerView.Adapter<RecyclerTripSchedule.ViewHolder> {
    private ArrayList<String> tData = null;
    private ArrayList<String> pData = null;
    Context context;

    public class ViewHolder extends RecyclerView.ViewHolder {
        TextView tripPosition;
        TextView tripSchedule;

        ViewHolder(View itemView) { //itemView를 저장하는 뷰홀더 클래스
            super(itemView);

            itemView.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    int pos = getAdapterPosition();
                    if (pos != RecyclerView.NO_POSITION){
                        //Intent intent = new Intent(RecyclerTripSchedule.this, TripScheduleDetail.class);
                        //startActivity(intent);
                        notifyItemChanged(pos);
                    }
                }
            });

            tripPosition = itemView.findViewById(R.id.tripPosition);
            tripSchedule = itemView.findViewById(R.id.tripSchedule);
        }
    }

    RecyclerTripSchedule(ArrayList<String> list){
        tData = list;
        pData = list;
    }

    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        context = parent.getContext();
        LayoutInflater inflater = (LayoutInflater) context.getSystemService(Context.LAYOUT_INFLATER_SERVICE);

        View view = inflater.inflate(R.layout.trip_schedule_recycler, parent, false);
        RecyclerTripSchedule.ViewHolder vh = new RecyclerTripSchedule.ViewHolder(view);

        return vh;
    }

    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, int position) {
        String text1 = tData.get(position);
        String text2 = pData.get(position);

        holder.tripSchedule.setText(text1);
        holder.tripPosition.setText(text2);
    }

    @Override
    public int getItemCount() {
        return tData.size();
    }


    public void addTrip(String data){
        tData.add(data);
    }

    public void addDetail(String data){
        pData.add(data);
    }
}