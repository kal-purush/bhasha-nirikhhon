package com.test.murphy.weatherapp;

import android.app.Fragment;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import com.test.murphy.weatherapp.model.WeatherConditions;

import butterknife.BindView;
import butterknife.ButterKnife;


/**
 * A simple {@link Fragment} subclass.
 * Use the {@link ConditionsFragment#newInstance} factory method to
 * create an instance of this fragment.
 */
public class ConditionsFragment extends Fragment {
    // the fragment initialization parameters
    private static final String ARG_WEATHER_CONDITIONS = "weatherConditions";

    private WeatherConditions weatherConditions;

    @BindView(R.id.temperatureText) TextView temperatureText;
    @BindView(R.id.conditionsText) TextView conditionsText;
    @BindView(R.id.locationText) TextView locationText;

    public ConditionsFragment() {
        // Required empty public constructor
    }

    /**
     * Use this factory method to create a new instance of
     * this fragment using the provided parameters.
     *
     * @param param1 Parameter 1.
     * @return A new instance of fragment ConditionsFragment.
     */
    public static ConditionsFragment newInstance(WeatherConditions param1) {
        ConditionsFragment fragment = new ConditionsFragment();
        Bundle args = new Bundle();
        args.putParcelable(ARG_WEATHER_CONDITIONS, param1);
        fragment.setArguments(args);
        return fragment;
    }

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        if (getArguments() != null) {
            weatherConditions = getArguments().getParcelable(ARG_WEATHER_CONDITIONS);
        }
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        ViewGroup rootView = (ViewGroup) inflater.inflate(R.layout.fragment_conditions, container, false);
        ButterKnife.bind(this, rootView);
        // Inflate the layout for this fragment
        return rootView;

    }

    public void setWeatherConditions(WeatherConditions weatherConditions) {
        this.weatherConditions = weatherConditions;
        updateConditionsLayout();
    }

    private void updateConditionsLayout() {
        //TODO: Fix the units thing
        WeatherActivity activity = (WeatherActivity) getActivity();
        temperatureText.setText(String.valueOf(weatherConditions.currentTemperature) + activity.getUnits().getText());
        conditionsText.setText(weatherConditions.currentConditions);
        locationText.setText(weatherConditions.location);
    }
}