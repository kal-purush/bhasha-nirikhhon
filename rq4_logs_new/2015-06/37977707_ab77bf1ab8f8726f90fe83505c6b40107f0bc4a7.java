package com.android.settings.euphoria;

import android.os.Bundle;
import android.preference.Preference;

import com.android.settings.SettingsPreferenceFragment;
import com.android.settings.R;

public class BreathingNotifications extends SettingsPreferenceFragment {

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        addPreferencesFromResource(R.xml.breathing_notifications);
    }
}