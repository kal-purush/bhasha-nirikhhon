package com.example.myapplication.views;


import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.EditText;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import com.arellomobile.mvp.MvpAppCompatFragment;
import com.arellomobile.mvp.presenter.InjectPresenter;
import com.example.myapplication.MainActivity;
import com.example.myapplication.R;
import com.example.myapplication.presenters.CarProfilePresenter;

public class CarProfileFragment extends MvpAppCompatFragment implements CarProfileIF {

    @InjectPresenter
    CarProfilePresenter presenter;

    public CarProfileFragment() {
        // Required empty public constructor
    }

    public static CarProfileFragment newInstance() {
        return new CarProfileFragment();
    }

    @Override
    public void onActivityCreated(@Nullable Bundle savedInstanceState) {
        super.onActivityCreated(savedInstanceState);
    }

    @Override
    public View onCreateView(@NonNull LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        View view = inflater.inflate(R.layout.payment_fragment, container, false);

        ((MainActivity) getActivity()).getBottomNavigationView().setVisibility(View.GONE);
        getActivity().findViewById(R.id.include).setVisibility(View.GONE);

        EditText carNumber = view.findViewById(R.id.car_number_etxt_id);

        return view;
    }
}