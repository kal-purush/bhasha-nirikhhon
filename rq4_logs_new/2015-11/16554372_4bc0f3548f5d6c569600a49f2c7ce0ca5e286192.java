package com.joymeter.androidclient.picker.dialog;

import android.app.TimePickerDialog;
import android.content.Context;
import android.widget.TimePicker;

/**
 * Created by hramovecchi on 24/11/2015.
 */
public class DurationPickerDialog extends TimePickerDialog {

    public DurationPickerDialog(Context context, TimePickerDialog.OnTimeSetListener callBack, int hourOfDay, int minute, boolean is24HourView){
        super(context, callBack, hourOfDay, minute, is24HourView);
    }

    @Override
    public void onTimeChanged(TimePicker view, int hourOfDay, int minute) {
    }
}