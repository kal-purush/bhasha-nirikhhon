package com.it.mobile.hansa.hbm.fragments;


import android.os.Bundle;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import com.it.mobile.hansa.hbm.R;

/**
 * A simple {@link Fragment} subclass.
 */
public class RealizadoMain extends Fragment {


    public RealizadoMain() {
        // Required empty public constructor
    }


    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        // Inflate the layout for this fragment
        return inflater.inflate(R.layout.fragment_realizado_main, container, false);
    }

}