package com.example.raulstriglio.dagger2poc.fragments;


import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;
import android.widget.Toast;

import com.example.raulstriglio.dagger2poc.R;
import com.example.raulstriglio.dagger2poc.model.NewClass;

import javax.inject.Inject;
import javax.inject.Named;

import dagger.android.support.AndroidSupportInjection;

/**
 * Created by raul.striglio on 29/11/17.
 */

public class OtherFragment extends Fragment {

    @Inject
    @Named("fomartString3")
    NewClass newClass2;

    TextView textView;

    public static OtherFragment newInstance() {

        Bundle args = new Bundle();

        OtherFragment fragment = new OtherFragment();
        fragment.setArguments(args);
        return fragment;
    }

    @Override
    public void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        AndroidSupportInjection.inject(this);
    }

    @Override
    public void onActivityCreated(@Nullable Bundle savedInstanceState) {
        super.onActivityCreated(savedInstanceState);
    }

    @Nullable
    @Override
    public View onCreateView(LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState) {
        View mRootView = inflater.inflate(R.layout.sub_fragment, container, false);

        textView = mRootView.findViewById(R.id.textView);

        return mRootView;
    }

    @Override
    public void onResume() {
        super.onResume();

        textView.setText("Other Fragment");

        Toast.makeText(getContext(), newClass2.getMessage(), Toast.LENGTH_SHORT).show();
    }

}