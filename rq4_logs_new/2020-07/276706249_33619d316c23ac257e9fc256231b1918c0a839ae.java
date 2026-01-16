package com.dtavana.foodswipe.fragments;

import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.fragment.app.Fragment;

import com.dtavana.foodswipe.R;
import com.dtavana.foodswipe.databinding.FragmentErrorBinding;

public class ErrorFragment extends Fragment {

    public static final String TAG = "ErrorFragment";

    FragmentErrorBinding binding;

    String errorMessage;
    String buttonText;
    ALL_DESTINATIONS destination;

    public enum ALL_DESTINATIONS {
        FILTER
    }

    public ErrorFragment() {}

    public static ErrorFragment newInstance(String errorMessage, ALL_DESTINATIONS destination, String buttonText) {
        Bundle args = new Bundle();;

        args.putString("errorMessage", errorMessage);
        args.putSerializable("destination", destination);
        args.putString("buttonText", buttonText);

        ErrorFragment fragment = new ErrorFragment();
        fragment.setArguments(args);

        return fragment;
    }

    @Nullable
    @Override
    public View onCreateView(@NonNull LayoutInflater inflater, @Nullable ViewGroup parent, @Nullable Bundle savedInstanceState) {
        binding = FragmentErrorBinding.inflate(getLayoutInflater(), parent, false);
        return binding.getRoot();
    }

    @Override
    public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
        Bundle args = getArguments();

        errorMessage = args.getString("errorMessage");
        destination = (ALL_DESTINATIONS) args.getSerializable("destination");
        buttonText = args.getString("buttonText");

        binding.tvErrorMessage.setText(errorMessage);
        binding.btnAction.setText(buttonText);
        binding.btnAction.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Fragment fragment;
                String fragmentTag;

                switch (destination) {
                    case FILTER:
                    default:
                        fragment = new FilterFragment();
                        fragmentTag = FilterFragment.TAG;
                        break;
                }
                getParentFragmentManager().beginTransaction().replace(R.id.flContainer, fragment, fragmentTag).addToBackStack(fragmentTag).commit();
            }
        });

    }
}