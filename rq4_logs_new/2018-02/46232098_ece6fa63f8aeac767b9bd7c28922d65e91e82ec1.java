/*
 * MindBell - Aims to give you a support for staying mindful in a busy life -
 *            for remembering what really counts
 *
 *     Copyright (C) 2014-2018 Uwe Damken
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.googlecode.mindbell.preference;

import android.app.AlertDialog;
import android.content.Context;
import android.util.AttributeSet;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.RadioButton;
import android.widget.TextView;

import com.googlecode.mindbell.R;

import java.util.ArrayList;
import java.util.List;

/**
 * RingtonePreference only works for ringtone sounds under control of the operating system not for app specifics sounds.
 *
 * SoundPickerPreference requires an entries array (texts to be displayed) and an entryValues array (filenames of the sounds) which
 * are both the same length. The first entry (NO_SOUND_INDEX) is expected to present no sound file.
 */

public class SoundPickerPreference extends ListPreferenceWithSummaryFix {

    private static int NO_SOUND_INDEX = 0;

    private List<SoundItem> soundItemList;

    private int currentIndex = 0; // default value if android:defaultValue is not set

    private TextView summaryTextView;

    public SoundPickerPreference(Context context, AttributeSet attrs) {
        super(context, attrs);

        CharSequence[] iconTextArray = getEntries();
        CharSequence[] iconFilenameArray = getEntryValues();

        if (iconTextArray == null || iconFilenameArray == null || iconTextArray.length != iconFilenameArray.length) {
            throw new IllegalStateException(
                    "IconPickerPreference requires an entries array and an entryValues array which are both the same length");
        }

        soundItemList = new ArrayList<SoundItem>();
        for (int i = 0; i < iconTextArray.length; i++) {
            SoundItem item = new SoundItem(iconTextArray[i], iconFilenameArray[i], false);
            soundItemList.add(item);
        }
    }

    public boolean isBellPicked() {
        return currentIndex != NO_SOUND_INDEX;
    }

    @Override
    protected void onBindView(View view) {
        super.onBindView(view);
        summaryTextView = (TextView) view.findViewById(R.id.summary);
        updateSummary();
    }

    /**
     * Update summary shown in the preference page. This includes a summary text (only).
     */
    private void updateSummary() {
        if (summaryTextView != null) { // set summary only if view is already bound
            SoundItem selectedSoundItem = soundItemList.get(currentIndex);
            summaryTextView.setText(selectedSoundItem.iconText);
        }
    }

    @Override
    protected void onDialogClosed(boolean positiveResult) {
        if (soundItemList != null) {
            for (int i = 0; i < soundItemList.size(); i++) {
                SoundItem item = soundItemList.get(i);
                if (item.isChecked) {
                    setValue(String.valueOf(i));
                    break;
                }
            }
        }
        super.onDialogClosed(positiveResult);
    }

    @Override
    public void setValue(final String value) {
        super.setValue(value);
        currentIndex = Integer.parseInt(value);
        for (int i = 0; i < soundItemList.size(); i++) {
            soundItemList.get(i).isChecked = (i == currentIndex);
        }
        updateSummary();
    }

    @Override
    protected void onSetInitialValue(boolean restoreValue, Object defaultValue) {
        String newValue = null;
        if (restoreValue) {
            if (defaultValue == null) {
                newValue = getPersistedString("0");
            } else {
                newValue = getPersistedString(defaultValue.toString());
            }
        } else {
            newValue = defaultValue.toString();
        }
        setValue(newValue);
    }

    @Override
    protected void onPrepareDialogBuilder(AlertDialog.Builder builder) {
        builder.setNegativeButton(android.R.string.cancel, null);
        builder.setPositiveButton(android.R.string.ok, null);
        CustomListPreferenceAdapter customListPreferenceAdapter =
                new CustomListPreferenceAdapter(getContext(), R.layout.sound_picker_preference_item, soundItemList);
        builder.setAdapter(customListPreferenceAdapter, null);
    }

    private class CustomListPreferenceAdapter extends ArrayAdapter<SoundItem> {

        private int resource;

        public CustomListPreferenceAdapter(Context context, int resource, List<SoundItem> objects) {
            super(context, resource, objects);
            this.resource = resource; // there is no way to get this back from super class
        }

        @Override
        public View getView(final int position, View convertView, ViewGroup parent) {

            if (convertView == null) {
                convertView = LayoutInflater.from(getContext()).inflate(resource, parent, false);
            }
            TextView soundText = (TextView) convertView.findViewById(R.id.soundText);
            RadioButton soundRadio = (RadioButton) convertView.findViewById(R.id.soundRadio);

            SoundItem soundItem = getItem(position);
            soundText.setText(soundItem.iconText);
            soundRadio.setChecked(soundItem.isChecked);

            convertView.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    for (int i = 0; i < getCount(); i++) {
                        getItem(i).isChecked = (i == position);
                    }
                    getDialog().dismiss();
                }
            });

            return convertView;
        }
    }

    private class SoundItem {

        private String iconText;

        private String soundFilename;

        private boolean isChecked;

        public SoundItem(CharSequence iconText, CharSequence soundFilename, boolean isChecked) {
            this.iconText = iconText.toString();
            this.soundFilename = soundFilename.toString();
            this.isChecked = isChecked;
        }

    }

}