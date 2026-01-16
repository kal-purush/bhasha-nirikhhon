package com.example.emergensui.automotive_ui.Adapter;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import com.example.emergensui.automotive_ui.Class.Patient;
import com.example.emergensui.automotive_ui.R;

import java.util.List;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

public class PatientChildAdapter extends RecyclerView.Adapter<PatientChildAdapter.PatientChildHolder>
{

    private Context context;
    List<Patient> lstPatient;
    Patient p;
    int position;
    int count = 0;

    public PatientChildAdapter(List<Patient> lsp, Context context)
    {
        this.context = context;
        this.lstPatient = lsp;

    }

    public PatientChildAdapter(Patient patient, Context context)
    {
        this.context = context;
        this.p = patient;

    }


    @NonNull
    @Override
    public PatientChildHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(context).inflate(R.layout.child_recycler_view, parent, false);
        return new PatientChildHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull PatientChildHolder holder, int position) {
        Patient pa = lstPatient.get(position);
        holder.setDetails(pa);
        //count++;





    }

    @Override
    public int getItemCount() {
        return (lstPatient == null)? 0 : lstPatient.size();
    }

    class PatientChildHolder extends RecyclerView.ViewHolder
    {

        private TextView txtPatientRoom;
        private TextView txtPatientType;
        private TextView txtPatientDoB;


        PatientChildHolder(@NonNull View itemView)
        {
            super(itemView);
            txtPatientRoom = itemView.findViewById(R.id.lblPatientRoom);
            txtPatientType = itemView.findViewById(R.id.lblPatientTypeValue);
            txtPatientDoB = itemView.findViewById(R.id.lblPatientDoB_Value);

        }

        void setDetails(Patient p)
        {
            txtPatientRoom.setText(p.getRoom_Number());
            txtPatientType.setText(p.getPatient_Type());
            txtPatientDoB.setText(p.getDoB());





        }
    }
}