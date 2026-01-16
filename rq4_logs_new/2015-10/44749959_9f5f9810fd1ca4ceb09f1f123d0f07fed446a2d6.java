/*
 * Copyright (C) 2010 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.android.settings;

import com.android.internal.view.RotationPolicy;
import com.android.settings.notification.DropDownPreference;
import com.android.settings.notification.DropDownPreference.Callback;
import com.android.settings.search.BaseSearchIndexProvider;
import com.android.settings.search.Indexable;

import android.app.Activity;
import android.app.ActivityManagerNative;
import android.app.Dialog;
import android.app.admin.DevicePolicyManager;
import android.content.ContentResolver;
import android.content.Context;
import android.content.res.Configuration;
import android.content.res.Resources;
import android.hardware.Sensor;
import android.hardware.SensorManager;
import android.os.Build;
import android.os.Bundle;
import android.os.RemoteException;
import android.os.SystemProperties;
import android.preference.CheckBoxPreference;
import android.preference.EditTextPreference;
import android.preference.ListPreference;
import android.preference.Preference;
import android.preference.Preference.OnPreferenceClickListener;
import android.preference.PreferenceActivity;
import android.preference.PreferenceScreen;
import android.preference.SwitchPreference;
import android.provider.SearchIndexableResource;
import android.provider.Settings;
import android.text.TextUtils;
import android.util.Log;

import java.util.ArrayList;
import java.util.List;

public class PhoneLabSettings extends SettingsPreferenceFragment implements
        Preference.OnPreferenceChangeListener {
    private static final String TAG = "PhoneLabSettings";

    private static final String KEY_NAME = "name";
    private static final String KEY_AGE = "age";
    private static final String KEY_LAPTOP = "have_laptop";
    private static final String KEY_DESKTOP = "have_laptop";
    private static final String KEY_ANOTHER_PHONE = "have_another_phone";

    private EditTextPreference mNamePreference;
    private CheckBoxPreference mLaptopPreference;
    private CheckBoxPreference mDesktopPreference;
    private CheckBoxPreference mAnotherPhonePreference;
    private ListPreference mAgePreference;

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        final Activity activity = getActivity();
        final ContentResolver resolver = activity.getContentResolver();

        addPreferencesFromResource(R.xml.phonelab_settings);
        updateState();
    }

    @Override
    public void onResume() {
        super.onResume();
        updateState();
    }

    @Override
    public void onPause() {
        super.onPause();
    }

    private void updateState() {
        mNamePreference = (EditTextPreference) init(mNamePreference, KEY_NAME);
        String name = null;
        if (mNamePreference != null) {
            name = mNamePreference.getText();
            updateName(name);
        }

        mAgePreference = (ListPreference) init(mAgePreference, KEY_AGE);
        if (mAgePreference != null) {
            String age = mAgePreference.getValue();
            mAgePreference.setSummary(age);
        }

        mLaptopPreference = (CheckBoxPreference) init(mLaptopPreference, KEY_LAPTOP);
        mDesktopPreference = (CheckBoxPreference) init(mDesktopPreference, KEY_DESKTOP);
        mAnotherPhonePreference = (CheckBoxPreference) init(mAnotherPhonePreference, KEY_ANOTHER_PHONE);

        Log.d(TAG, "update: " + name + ", " + mLaptopPreference.isChecked());
    }

    private Preference init(Preference preference, String key) {
        if (preference == null) {
            preference = findPreference(key);
            preference.setOnPreferenceChangeListener(this);
        }
        return preference;
    }

    private void updateName(String name) {
        if (name != null && !name.isEmpty()) {
            mNamePreference.setSummary(name);
        } else {
            mNamePreference.setSummary(R.string.your_name_summary);
        }
    }

    @Override
    public boolean onPreferenceChange(Preference preference, Object newValue) {
        Log.d(TAG, "onPreferenceChange: " + preference + " new value: " + newValue);
        if (preference == mNamePreference) {
            updateName(newValue.toString());
        } else if (preference == mAgePreference) {
            mAgePreference.setSummary(newValue.toString());
        }
        return true;
    }

    // @Override
    // public boolean onPreferenceChange(Preference preference, Object objValue) {
    //     final String key = preference.getKey();
    //     Log.d(TAG, "onPreferenceChange: " + key + " " + objValue);
    //     if (KEY_SCREEN_TIMEOUT.equals(key)) {
    //         try {
    //             int value = Integer.parseInt((String) objValue);
    //             Settings.System.putInt(getContentResolver(), SCREEN_OFF_TIMEOUT, value);
    //             updateTimeoutPreferenceDescription(value);
    //         } catch (NumberFormatException e) {
    //             Log.e(TAG, "could not persist screen timeout setting", e);
    //         }
    //     }
    //     if (KEY_FONT_SIZE.equals(key)) {
    //         writeFontSizePreference(objValue);
    //     }
    //     if (preference == mAutoBrightnessPreference) {
    //         boolean auto = (Boolean) objValue;
    //         Settings.System.putInt(getContentResolver(), SCREEN_BRIGHTNESS_MODE,
    //                 auto ? SCREEN_BRIGHTNESS_MODE_AUTOMATIC : SCREEN_BRIGHTNESS_MODE_MANUAL);
    //     }
    //     if (preference == mLiftToWakePreference) {
    //         boolean value = (Boolean) objValue;
    //         Settings.Secure.putInt(getContentResolver(), WAKE_GESTURE_ENABLED, value ? 1 : 0);
    //     }
    //     if (preference == mDozePreference) {
    //         boolean value = (Boolean) objValue;
    //         Settings.Secure.putInt(getContentResolver(), DOZE_ENABLED, value ? 1 : 0);
    //     }
    //     return true;
    // }
}