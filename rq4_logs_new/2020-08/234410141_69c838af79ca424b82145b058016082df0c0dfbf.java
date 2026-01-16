package com.apptec.camello.loginactivity;

import android.os.Bundle;

import androidx.annotation.Nullable;
import androidx.fragment.app.Fragment;
import androidx.lifecycle.ViewModelProvider;

/**
 * A class for write the similar code inside {@link FormFragment} and {@link ShowPoliciesFragment}
 */
public class BaseLoginFragment extends Fragment {

    // Instance of ViewModel
    protected LoginViewModel loginViewModel;


    @Override
    public void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        loginViewModel = new ViewModelProvider(getActivity()).get(LoginViewModel.class);            // Getting the view model

    }

}