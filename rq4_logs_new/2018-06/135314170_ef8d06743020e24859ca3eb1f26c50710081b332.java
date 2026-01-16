package com.example.mohammad.studentpa.Reminders;

import android.app.DatePickerDialog;
import android.app.DatePickerDialog.OnDateSetListener;
import android.app.Dialog;
import android.os.Bundle;
import android.support.v4.app.DialogFragment;
import android.widget.DatePicker;
import android.widget.TextView;

import com.example.mohammad.studentpa.R;

import java.util.Calendar;

public class DatePickerFragment extends DialogFragment implements OnDateSetListener {

    @Override
    public Dialog onCreateDialog(Bundle savedInstanceState) {
        // Use the current date as the default date in the picker
        final Calendar c = Calendar.getInstance();
        int year = c.get(Calendar.YEAR);
        int month = c.get(Calendar.MONTH);
        int day = c.get(Calendar.DAY_OF_MONTH);



        // Create a new instance of DatePickerDialog and return it
        return new DatePickerDialog(getActivity(), this, year, month, day);
    }

    public void onDateSet(DatePicker view, int year, int month, int day) {
        //set date to textView
        String reminderDate = day + " / " + month + " / " + year;
        TextView textViewDate = getActivity().findViewById(R.id.textViewReminderDate);
        textViewDate.setText(reminderDate);

    }

}