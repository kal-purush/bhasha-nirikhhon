package com.example.emergensui.automotive_ui.Adapter;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import com.example.emergensui.automotive_ui.Class.Patient;
import com.example.emergensui.automotive_ui.R;

import java.util.ArrayList;
import java.util.List;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

public class PatientAdapter extends RecyclerView.Adapter<PatientAdapter.PatientHolder>
{

    private Context context;
    private List<Patient> lstPatient;

    private boolean visible = true;

    public PatientAdapter(Context context, ArrayList<Patient> lstp)
    {
        this.context = context;
        this.lstPatient = lstp;
    }

    @NonNull
    @Override
    public PatientHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(context).inflate(R.layout.content_recycleview, parent, false);
        return new PatientHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull PatientHolder holder, int position) {
        Patient p = lstPatient.get(position);
        holder.setDetails(p);






    }

    @Override
    public int getItemCount() {
        return lstPatient.size();
    }

    class PatientHolder extends RecyclerView.ViewHolder
    {

        private TextView txtPatientName;
        private TextView txtPatientRoom;
        private TextView txtPatientType;
        private TextView txtPatientDoB;

        PatientHolder(@NonNull View itemView)
        {
            super(itemView);
            txtPatientName = itemView.findViewById(R.id.lbPatientName);
            txtPatientRoom = itemView.findViewById(R.id.lblPatientRoom);
            txtPatientType = itemView.findViewById(R.id.lblPatientTypeValue);
            txtPatientDoB = itemView.findViewById(R.id.lblPatientDoB_Value);
        }

        void setDetails(Patient p)
        {
            txtPatientName.setText(p.getPatient_Name());
            txtPatientRoom.setText(p.getRoom_Number());
            txtPatientType.setText(p.getPatient_Type());
            txtPatientDoB.setText(p.getDoB());

        }
    }
}