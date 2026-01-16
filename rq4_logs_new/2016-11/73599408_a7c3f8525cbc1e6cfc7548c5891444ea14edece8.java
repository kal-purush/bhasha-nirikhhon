package com.example.jason.linearalgebraapp.Activities.Fragments;

import android.os.Bundle;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import com.example.jason.linearalgebraapp.R;

/**
 * Created by jason on 11/13/2016.
 */

public class DefaultFragment extends Fragment {
    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {

        View rootView = inflater.inflate(R.layout.fragment_default, container, false);
        return rootView;
    }

    public DefaultFragment() {}

}