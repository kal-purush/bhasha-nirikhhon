package com.evolver.chiron.user;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.evolver.chiron.R;

import java.util.List;

public class UserVaccineAdapter extends RecyclerView.Adapter<UserVaccineAdapter.ViewHolder>{

    List<UserVaccineModel> centerList;
    public UserVaccineAdapter(List<UserVaccineModel> centerList){
        this.centerList = centerList;
    }

    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.user_vaccine_item_design,parent,false);
        return new ViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, int position) {
        UserVaccineModel currentItem = centerList.get(position);

        holder.tvCenterName.setText(currentItem.centerName);
        holder.tvCenterAddress.setText(currentItem.centerAddress);
        holder.tvAgeLimit.setText(currentItem.ageLimit);
        holder.tvAvailability.setText(currentItem.availableCapacity);
        holder.tvCenterTiming.setText(""+currentItem.centerFromTiming+" to "+currentItem.centerToTiming);
        holder.tvVaccineName.setText(currentItem.vaccineName);
        holder.tvFeeType.setText(currentItem.feeType);
    }

    @Override
    public int getItemCount() {
        return centerList.size();
    }

    public class ViewHolder extends RecyclerView.ViewHolder{

        TextView tvCenterName;
        TextView tvCenterAddress;
        TextView tvAgeLimit;
        TextView tvAvailability;
        TextView tvCenterTiming;
        TextView tvVaccineName;
        TextView tvFeeType;
        public ViewHolder(@NonNull View itemView) {
            super(itemView);
            tvCenterName = itemView.findViewById(R.id.tv_center_name);
            tvCenterAddress = itemView.findViewById(R.id.tv_center_address);
            tvAgeLimit = itemView.findViewById(R.id.tv_age_limit);
            tvAvailability = itemView.findViewById(R.id.tv_availability);
            tvCenterTiming = itemView.findViewById(R.id.tv_center_timing);
            tvVaccineName = itemView.findViewById(R.id.tv_vaccine_name);
            tvFeeType = itemView.findViewById(R.id.tv_fee_type);
        }
    }
}