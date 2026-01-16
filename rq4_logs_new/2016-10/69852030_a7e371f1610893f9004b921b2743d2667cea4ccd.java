package com.byteshaft.shiurim.fragments;


import android.app.Fragment;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import com.byteshaft.shiurim.R;

public class CalendarFragment extends android.support.v4.app.Fragment {

    private View mBaseView;

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        mBaseView = inflater.inflate(R.layout.calendar_fragment, container, false);
        return mBaseView;
    }
}