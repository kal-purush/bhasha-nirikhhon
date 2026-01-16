package com.joymeter.androidclient.dialog;

import android.app.Activity;
import android.app.Dialog;
import android.app.DialogFragment;
import android.app.Fragment;
import android.app.TimePickerDialog;
import android.content.Intent;
import android.os.Bundle;
import android.text.format.DateFormat;
import android.widget.TimePicker;

import java.util.Calendar;

/**
 * Created by hramovecchi on 12/11/2015.
 */
public class DurationPickerFragment extends DialogFragment
        implements TimePickerDialog.OnTimeSetListener {

    private Calendar c;

    @Override
    public Dialog onCreateDialog(Bundle savedInstanceState) {
        Bundle setDuration = this.getArguments();

        int hour = setDuration.getInt("hours");
        int minute = setDuration.getInt("minutes");

       TimePickerDialog durationPicker =  new TimePickerDialog(getActivity(), this, hour, minute,
                DateFormat.is24HourFormat(getActivity()));

        durationPicker.setTitle("Duration:");

        return durationPicker;
    }

    public void onTimeSet(TimePicker view, int hourOfDay, int minute) {

        Intent resultIntent = new Intent();
        resultIntent.putExtra("DURATION_HOURS_PICKED", hourOfDay);
        resultIntent.putExtra("DURATION_MINUTES_PICKED", minute);

        Fragment f = getTargetFragment();
        f.onActivityResult(getTargetRequestCode(), Activity.RESULT_OK, resultIntent);
    }
}