package com.example.myapplication.preferences;


import android.content.Context;
import android.util.AttributeSet;
import android.util.Log;
import android.widget.Switch;

import androidx.preference.PreferenceViewHolder;
import androidx.preference.SwitchPreference;

import com.example.myapplication.R;

public class AppGeolocationPreference extends SwitchPreference {

    public AppGeolocationPreference(Context context, AttributeSet attrs, int defStyleAttr, int defStyleRes) {
        super(context, attrs, defStyleAttr, defStyleRes);
    }

    public AppGeolocationPreference(Context context, AttributeSet attrs, int defStyleAttr) {
        super(context, attrs, defStyleAttr);
    }

    public AppGeolocationPreference(Context context, AttributeSet attrs) {
        super(context, attrs);
    }

    public AppGeolocationPreference(Context context) {
        super(context);
    }

    @Override
    public void onBindViewHolder(PreferenceViewHolder holder) {
        super.onBindViewHolder(holder);
        Switch aSwitch = (Switch) holder.findViewById(R.id.geolocation_switch_id);
        if (aSwitch != null) {
            aSwitch.setChecked(isChecked());
        }
    }
}