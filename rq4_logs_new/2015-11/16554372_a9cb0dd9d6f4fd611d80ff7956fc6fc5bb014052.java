package com.joymeter.androidclient.dialog;

import android.app.Activity;
import android.app.Dialog;
import android.app.DialogFragment;
import android.app.Fragment;
import android.app.TimePickerDialog;
import android.content.DialogInterface;
import android.content.Intent;
import android.os.Bundle;
import android.text.format.DateFormat;
import android.widget.TimePicker;

import java.util.Calendar;

/**
 * Created by hernan.ramovecchi on 12/11/2015.
 */
public class TimePickerFragment extends DialogFragment
        implements TimePickerDialog.OnTimeSetListener {

    private Calendar c;

    @Override
    public Dialog onCreateDialog(Bundle savedInstanceState) {
        Bundle setDate = this.getArguments();

        c = Calendar.getInstance();
        c.setTimeInMillis(setDate.getLong("date"));

        int hour = c.get(Calendar.HOUR_OF_DAY);
        int minute = c.get(Calendar.MINUTE);

        // Create a new instance of TimePickerDialog and return it
        return new TimePickerDialog(getActivity(), this, hour, minute,
                DateFormat.is24HourFormat(getActivity()));
    }

    public void onTimeSet(TimePicker view, int hourOfDay, int minute) {
        c.set(Calendar.HOUR_OF_DAY, hourOfDay);
        c.set(Calendar.MINUTE, minute);

        Intent resultIntent = new Intent();
        resultIntent.putExtra("DATE_AND_TIME_PICKED", c.getTimeInMillis());

        Fragment f = getTargetFragment();
        f.onActivityResult(getTargetRequestCode(), Activity.RESULT_OK, resultIntent);
    }
}