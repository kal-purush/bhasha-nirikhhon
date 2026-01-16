package com.teamachievers.medix.Adapter;

import android.content.Context;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;
import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentManager;
import androidx.fragment.app.FragmentTransaction;
import androidx.recyclerview.widget.RecyclerView;

import com.squareup.picasso.Picasso;
import com.teamachievers.medix.Model.clinicsModel;
import com.teamachievers.medix.Model.doctorsModel;
import com.teamachievers.medix.MyAppointment;
import com.teamachievers.medix.R;
import com.teamachievers.medix.Schedule1_D;

import java.util.ArrayList;

public class scheduleDAdapter extends RecyclerView.Adapter<scheduleDAdapter.ViewHolder> {

    private ArrayList<doctorsModel> scheduleArrayList;
    private Context context;

    public scheduleDAdapter(ArrayList<doctorsModel> scheduleArrayList, Context context) {
        this.scheduleArrayList = scheduleArrayList;
        this.context = context;
    }

    @NonNull
    @Override
    public scheduleDAdapter.ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        return new ViewHolder(LayoutInflater.from(context).inflate(R.layout.card_doctor, parent, false));
    }

    @Override
    public void onBindViewHolder(@NonNull scheduleDAdapter.ViewHolder holder, int position) {
        doctorsModel model = scheduleArrayList.get(position);

        holder.doctorName.setText(model.getDoctorName());
        Picasso.get().load(model.getDoctorImage()).into(holder.doctorImage);
        holder.doctorImage.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Fragment fragment = new MyAppointment();
                Bundle bundle = new Bundle();
                FragmentManager fragmentManager = ((AppCompatActivity) context).getSupportFragmentManager();
                FragmentTransaction fragmentTransaction = fragmentManager.beginTransaction();
                bundle.putString("cid", String.valueOf(position));
                bundle.putString("did", String.valueOf(position));
                fragment.setArguments(bundle);
                fragmentTransaction.replace(R.id.frameContainer2, fragment);
                fragmentTransaction.addToBackStack(null);
                fragmentTransaction.commit();
            }
        });



}

    @Override
    public int getItemCount() {
        return scheduleArrayList.size();
    }

    class ViewHolder extends RecyclerView.ViewHolder {

        TextView doctorName;
        ImageView doctorImage;

        public ViewHolder(@NonNull View itemView) {
            super(itemView);
            doctorName = itemView.findViewById(R.id.doctor_name);
            doctorImage = itemView.findViewById(R.id.doctor_image);

        }
    }
}