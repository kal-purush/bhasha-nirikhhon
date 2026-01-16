package com.bytearray.pledge.activity;

import android.app.Fragment;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageButton;

import com.bytearray.pledge.R;

/**
 * Created by adh on 9/19/2015.
 */
public class FragmentUserRegister extends Fragment {

    ImageButton gpsTrigger;

    public FragmentUserRegister() {
        // Required empty public constructor
    }

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,Bundle savedInstanceState) {
        View rootView = inflater.inflate(R.layout.user_registration, container, false);

        gpsTrigger = (ImageButton) rootView.findViewById(R.id.imageButtonGPS);
        gpsTrigger.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                
            }
        });



        return rootView;
    }
}