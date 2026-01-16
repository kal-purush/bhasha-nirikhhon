package com.compostela.curso.terremotos;

import android.os.Bundle;
import android.preference.PreferenceFragment;

/**
 * Created by mañá on 06/02/2015.
 */
public class SettingsFragment extends PreferenceFragment{

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        addPreferencesFromResource(R.xml.preferencias);
    }
}