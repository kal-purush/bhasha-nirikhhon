package com.test.murphy.weatherapp;

import android.app.Fragment;
import android.content.Context;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ToggleButton;

import butterknife.BindView;
import butterknife.ButterKnife;
import butterknife.OnClick;


public class ButtonsFragment extends Fragment {
    private OnFragmentInteractionListener mListener;

    @BindView(R.id.unitsToggle)
    ToggleButton unitsToggle;

    public ButtonsFragment() {
        // Required empty public constructor
    }

    public static ButtonsFragment newInstance() {
        ButtonsFragment fragment = new ButtonsFragment();
        return fragment;
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        // Inflate the layout for this fragment
        ViewGroup rootView = (ViewGroup) inflater.inflate(R.layout.fragment_buttons, container, false);
        ButterKnife.bind(this, rootView);
        // Inflate the layout for this fragment
        return rootView;
    }

    @OnClick(R.id.aboutButton)
    public void aboutButtonTapped() {
        if (mListener != null) {
            mListener.onAboutButtonTapped();
        }
    }

    @OnClick(R.id.unitsToggle)
    public void unitsToggleTapped() {
        if (mListener != null) {
            mListener.onUnitToggleTapped();
        }
    }

    @OnClick(R.id.locationButton)
    public void locationButtonTapped() {
        if (mListener != null) {
            mListener.onLocationButtonTapped();
        }
    }

    @Override
    public void onAttach(Context context) {
        super.onAttach(context);
        if (context instanceof OnFragmentInteractionListener) {
            mListener = (OnFragmentInteractionListener) context;
        } else {
            throw new RuntimeException(context.toString()
                    + " must implement OnFragmentInteractionListener");
        }
    }

    @Override
    public void onDetach() {
        super.onDetach();
        mListener = null;
    }

    public boolean isUnitsToggleChecked() {
        return unitsToggle.isChecked();
    }

    public interface OnFragmentInteractionListener {
        void onAboutButtonTapped();
        void onUnitToggleTapped();
        void onLocationButtonTapped();
    }
}