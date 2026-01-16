package com.it.mobile.hansa.hbm.fragments;


import android.os.Bundle;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import com.google.android.gms.maps.SupportMapFragment;
import com.it.mobile.hansa.hbm.R;

/**
 * A simple {@link Fragment} subclass.
 */
public class MappMain extends SupportMapFragment {


    public MappMain() {
        // Required empty public constructor
    }


    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        // Inflate the layout for this fragment
        return inflater.inflate(R.layout.fragment_mapp_main, container, false);
    }

}