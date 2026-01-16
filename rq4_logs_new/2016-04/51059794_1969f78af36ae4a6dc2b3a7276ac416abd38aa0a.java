package com.ikode.fragments;

import android.support.v4.app.Fragment;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import com.ikode.viezara.ikode.R;

/**
 * Created by viezara on 11/04/2016.
 */
public class FragmentAbout extends Fragment{

    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        View v = inflater.inflate(R.layout.activity_about_us,container,false);
        return v;
    }
}