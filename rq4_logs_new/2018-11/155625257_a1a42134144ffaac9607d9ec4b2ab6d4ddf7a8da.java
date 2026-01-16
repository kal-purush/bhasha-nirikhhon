package group6.tcss450.uw.edu.chatapp.utils;


import android.os.Bundle;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import group6.tcss450.uw.edu.chatapp.R;

/**
 * A simple {@link Fragment} subclass.
 */
public class Weather_Fragment extends Fragment {


    public Weather_Fragment() {
        // Required empty public constructor
    }


    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        // Inflate the layout for this fragment
        return inflater.inflate(R.layout.fragment_weather_, container, false);
    }

}