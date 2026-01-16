package com.example.edugateguru.Fragment;

import android.content.Intent;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.cardview.widget.CardView;
import androidx.fragment.app.Fragment;

import com.example.edugateguru.ProfileActivity;
import com.example.edugateguru.R;
import com.example.edugateguru.TugasActivity;

public class FragmentHome extends Fragment implements View.OnClickListener{

    CardView cvAgenda,cvProfile,cvAbout,cvTugas;

    @Nullable
    @Override
    public View onCreateView(@NonNull LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState) {
        LayoutInflater If = getActivity().getLayoutInflater() ;
        View view = If.inflate(R.layout.fragment_home,container,false);
        return view;
    }

    @Override
    public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {

        cvAgenda = getView().findViewById(R.id.agenda);
        cvAgenda.setOnClickListener(this);

        cvProfile = getView().findViewById(R.id.profile);
        cvProfile.setOnClickListener(this);

        cvAbout = getView().findViewById(R.id.agenda);
        cvAbout.setOnClickListener(this);

        cvTugas = getView().findViewById(R.id.tugas);
        cvTugas.setOnClickListener(this);

    }

    @Override
    public void onClick(View view) {
        switch (view.getId()) {
            case R.id.tugas:
                startActivity(new Intent(getActivity().getApplication(), TugasActivity.class));
                break;
            case R.id.profile:
                startActivity(new Intent(getActivity().getApplication(), ProfileActivity.class));
                break;
        }
    }
}