package com.mrezanasirloo.preferencezero.sample;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;

import com.mrezanasirloo.preferencezero.preferences.PreferenceCheckBox;
import com.mrezanasirloo.preferencezero.preferences.PreferenceSwitch;

public class MainActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        PreferenceSwitch tempSwitch = (PreferenceSwitch) findViewById(R.id.preferences_measurement_unit);
        tempSwitch.setOffText("F");
        tempSwitch.setOnText("C");

        PreferenceSwitch clockSwitch = (PreferenceSwitch) findViewById(R.id.preferences_clock_format);
        clockSwitch.setOffText("24");
        clockSwitch.setOnText("12");
        PreferenceCheckBox checkBox = (PreferenceCheckBox) findViewById(R.id.preferences_auto_update);
    }
}