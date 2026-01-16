package com.example.jason.linearalgebraapp.Activities.Fragments;

import android.os.Bundle;
import android.support.v4.app.Fragment;
import android.support.v4.app.FragmentTransaction;


/**
 * Created by jason on 11/13/2016.
 */

public class OperationDetailsFragment extends Fragment {
    private static String OPERATION_INDEX = "OPERATION INDEX KEY";


    public static OperationDetailsFragment newInstance(int index) {
        OperationDetailsFragment fragment = new OperationDetailsFragment();

        Bundle arguments = new Bundle();
        arguments.putInt(OPERATION_INDEX, index);

        fragment.setArguments(arguments);
        FragmentTransaction transaction = getSupportFragmentManager().beginTransaction();

        return fragment;
    }
}