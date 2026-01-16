package com.example.oguz.whatsaround;

import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;

/**
 * Created by oguz on 23.05.2017.
 */

public class FirmaLoginFirstFragment extends Fragment {
    @Nullable
    @Override
    public View onCreateView(LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState) {
        View v = inflater.inflate(R.layout.fragment_firma_login,container,false);

        Button btnSignup = (Button) v.findViewById(R.id.btnSingup);
        btnSignup.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Fragment fr  = new FirmaSignupFragment();
                getActivity().getSupportFragmentManager()
                        .beginTransaction().replace(R.id.fragment_container,fr).addToBackStack("login").commit();
            }
        });

        return v;
    }
}