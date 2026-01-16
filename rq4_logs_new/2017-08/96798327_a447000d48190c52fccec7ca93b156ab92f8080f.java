package com.tvnsoftware.drcare.activity;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.DefaultItemAnimator;
import android.support.v7.widget.DividerItemDecoration;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.widget.Button;

import com.tvnsoftware.drcare.R;
import com.tvnsoftware.drcare.adapter.PrescriptionAdapter;
import com.tvnsoftware.drcare.model.medicalrecord.MedicalRecord;

import butterknife.BindView;
import butterknife.ButterKnife;

public class DiagnosisDetailActivity extends AppCompatActivity {
    private PrescriptionAdapter prescriptionAdapter;
    private LinearLayoutManager mLayoutManager;
    @BindView(R.id.rc_prescription)
    RecyclerView mRcPrescription;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_diagnosis_detail);
        ButterKnife.bind(this);

        MedicalRecord medicalRecord = getIntent().getExtras().getParcelable(DiagnosisActivity.EXTRA_PATIENT);
        prescriptionAdapter = new PrescriptionAdapter(this, medicalRecord.getPrescriptionList());
        mLayoutManager = new LinearLayoutManager(getBaseContext(), LinearLayoutManager.VERTICAL, false);
        DividerItemDecoration dividerItemDecoration = new DividerItemDecoration(prescriptionAdapter.getContext(),
                mLayoutManager.getOrientation());
        mRcPrescription.addItemDecoration(dividerItemDecoration);
        mRcPrescription.setLayoutManager(mLayoutManager);
        mRcPrescription.setItemAnimator(new DefaultItemAnimator());
        mRcPrescription.setAdapter(prescriptionAdapter);
    }
}