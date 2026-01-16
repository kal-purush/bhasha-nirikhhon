package utn.proy2k18.vantrack.utils;

import android.app.DatePickerDialog;
import android.app.TimePickerDialog;
import android.support.v4.app.FragmentActivity;
import android.widget.DatePicker;
import android.widget.TextView;
import android.widget.TimePicker;

import java.util.Calendar;
import java.util.Locale;

public class DateTimePicker {
    private Calendar calendar;
    private FragmentActivity activity;

    public DateTimePicker(FragmentActivity activity) {
        this.activity = activity;
        this.calendar = Calendar.getInstance();
    }

    public void pickDate(final TextView textView) {
        int day = this.calendar.get(Calendar.DAY_OF_MONTH);
        final int month = this.calendar.get(Calendar.MONTH);
        final int year = this.calendar.get(Calendar.YEAR);

        DatePickerDialog datePickerDialog = new DatePickerDialog(this.activity, new DatePickerDialog.OnDateSetListener() {
            @Override
            public void onDateSet(DatePicker view, int yearSelected, int monthOfYearSelected, int dayOfMonthSelected) {
                textView.setText(String.format(Locale.getDefault(),"%02d-%02d-%02d",
                        dayOfMonthSelected, monthOfYearSelected + 1, yearSelected));
            }
        }, day, month, year);
        datePickerDialog.updateDate(year, month, day);
        datePickerDialog.show();
    }

    public void pickTime(final TextView textView) {
        int hour = this.calendar.get(Calendar.HOUR_OF_DAY);
        int minute = this.calendar.get(Calendar.MINUTE);

        TimePickerDialog mTimePicker;
        mTimePicker = new TimePickerDialog(this.activity, new TimePickerDialog.OnTimeSetListener() {
            @Override
            public void onTimeSet(TimePicker timePicker, int selectedHour, int selectedMinute) {
                textView.setText( selectedHour + ":" + selectedMinute);
            }
        }, hour, minute, true);
        mTimePicker.setTitle("Select Time");
        mTimePicker.show();
    }
}