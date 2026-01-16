package com.example.ldt.fragments;

import android.content.Context;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import androidx.fragment.app.Fragment;
import android.widget.ImageView;
import com.example.ldt.R;

/**
 * LightsFragment handles the UI and logic for the Lights section.
 */
public class LightsFragment extends Fragment {

    private ImageView ivArrowOn, ivArrowOff, ivOnText, ivOffText, ivSleepAnimation;
    private boolean isLightsOn = true; // State of lights
    private SharedPreferences sharedPreferences;

    public LightsFragment() {
        // Required empty public constructor
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        // Inflate the layout for this fragment
        View view = inflater.inflate(R.layout.fragment_lights, container, false);

        // Initialize views from the layout
        ivArrowOn = view.findViewById(R.id.iv_arrow_on);
        ivArrowOff = view.findViewById(R.id.iv_arrow_off);
        ivOnText = view.findViewById(R.id.iv_on_text);
        ivOffText = view.findViewById(R.id.iv_off_text);
        ivSleepAnimation = view.findViewById(R.id.iv_sleep_animation);

        // Set up SharedPreferences
        sharedPreferences = getActivity().getSharedPreferences("LightPrefs", Context.MODE_PRIVATE);

        // Check if a preference exists; if not, default to lights on
        isLightsOn = sharedPreferences.getBoolean("isLightsOn", true); // Default to true (lights on) if the preference doesn't exist

        // Set initial visibility based on the retrieved preference
        updateLightsState(isLightsOn);

        // Set click listeners for on_text and off_text
        ivOnText.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                isLightsOn = true; // Set lights on
                updateLightsState(isLightsOn);

                // Save the state to SharedPreferences
                sharedPreferences.edit().putBoolean("isLightsOn", true).apply();
            }
        });

        ivOffText.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                isLightsOn = false; // Set lights off
                updateLightsState(isLightsOn);

                // Save the state to SharedPreferences
                sharedPreferences.edit().putBoolean("isLightsOn", false).apply();
            }
        });

        return view;
    }

    /**
     * This method updates the visibility of the arrows, text, and sleep animation based on the light state.
     * @param lightsOn true if lights are on, false if lights are off
     */
    private void updateLightsState(boolean lightsOn) {
        if (lightsOn) {
            ivArrowOn.setVisibility(View.VISIBLE);
            ivArrowOff.setVisibility(View.INVISIBLE);
            ivOnText.setVisibility(View.VISIBLE);
            ivOffText.setVisibility(View.VISIBLE);
            ivSleepAnimation.setVisibility(View.INVISIBLE);
        } else {
            ivArrowOn.setVisibility(View.INVISIBLE);
            ivArrowOff.setVisibility(View.VISIBLE);
            ivOnText.setVisibility(View.VISIBLE);
            ivOffText.setVisibility(View.VISIBLE);
            ivSleepAnimation.setVisibility(View.INVISIBLE);
        }
    }
}