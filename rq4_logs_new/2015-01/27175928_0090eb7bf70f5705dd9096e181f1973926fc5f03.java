package edu.upc.eetac.dsa.dsaqt1415g4.utroll;

import android.app.Activity;
import android.app.DatePickerDialog;
import android.app.Dialog;
import android.app.ProgressDialog;
import android.app.TimePickerDialog;
import android.content.Intent;
import android.os.AsyncTask;
import android.os.Bundle;
import android.support.v4.app.DialogFragment;
import android.support.v4.app.FragmentActivity;
import android.text.format.DateFormat;
import android.util.Log;
import android.view.View;
import android.widget.DatePicker;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.TimePicker;

import com.google.gson.Gson;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import edu.upc.eetac.dsa.dsaqt1415g4.utroll.api.AppException;
import edu.upc.eetac.dsa.dsaqt1415g4.utroll.api.Group;
import edu.upc.eetac.dsa.dsaqt1415g4.utroll.api.uTrollAPI;

public class CreateGroupActivity extends FragmentActivity {
    private final static String TAG = CreateGroupActivity.class.getName();
    private static TextView tvDate = null;
    private static TextView tvTime = null;
    private static TextView tvDateClosing = null;
    private static TextView tvTimeClosing = null;

    private class CreateGroupTask extends AsyncTask<String, Void, Group> {
        private ProgressDialog pd;

        @Override
        protected Group doInBackground(String... params) {
            Group group = null;
            try {
                String groupname = params[0];
                int price = Integer.parseInt(params[1]);

                String date = params[2]; //"2014-12-31 21:27:00";
                String dateClosing = params[3];

                group = uTrollAPI.getInstance(CreateGroupActivity.this).createGroup(groupname, price, date, dateClosing);

            } catch (AppException e) {
                Log.e(TAG, e.getMessage(), e);
            }
            return group;
        }

        @Override
        protected void onPostExecute(Group result) {
            showGroups(result);
            if (pd != null) {
                pd.dismiss();
            }
        }

        @Override
        protected void onPreExecute() {
            pd = new ProgressDialog(CreateGroupActivity.this);

            pd.setCancelable(false);
            pd.setIndeterminate(true);
            pd.show();
        }
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.create_group_layout);
    }

    public class DatePickerFragment extends DialogFragment
            implements DatePickerDialog.OnDateSetListener {

        @Override
        public Dialog onCreateDialog(Bundle savedInstanceState) {
            // Use the current date as the default date in the picker
            final Calendar c = Calendar.getInstance();
            int year = c.get(Calendar.YEAR);
            int month = c.get(Calendar.MONTH);
            int day = c.get(Calendar.DAY_OF_MONTH);

            DatePickerDialog dialog = new DatePickerDialog(getActivity(), this, year, month, day);
            Date date = new Date();
            date.setTime(date.getTime() - 1000);
            dialog.getDatePicker().setMinDate(date.getTime());
            Date date1 = new Date();
            date1.setTime(date.getTime() + 172800000);
            dialog.getDatePicker().setMaxDate(date1.getTime());

            return dialog;
        }

        public void onDateSet(DatePicker view, int year, int month, int day) {
            tvDate = (TextView) findViewById(R.id.tvCreateGroupDate);

            tvDate.setText(new StringBuilder()
                     // Month is 0 based, just add 1
                     .append(year).append("-").append(month + 1).append("-")
                     .append(day));
        }
    }

    public void setDate(View v) {
        DialogFragment newFragment = new DatePickerFragment();
        newFragment.show(getSupportFragmentManager(), "datePicker");
    }

    public class TimePickerFragment extends DialogFragment
            implements TimePickerDialog.OnTimeSetListener {

        @Override
        public Dialog onCreateDialog(Bundle savedInstanceState) {
            // Use the current time as the default values for the picker
            final Calendar c = Calendar.getInstance();
            int hour = c.get(Calendar.HOUR_OF_DAY);
            int minute = c.get(Calendar.MINUTE);

            // Create a new instance of TimePickerDialog and return it
            return new TimePickerDialog(getActivity(), this, hour, minute,
                    DateFormat.is24HourFormat(getActivity()));
        }

        public void onTimeSet(TimePicker view, int hourOfDay, int minute) {
            tvTime = (TextView) findViewById(R.id.tvCreateGroupTime);

            tvTime.setText(new StringBuilder().append(hourOfDay).append(":").append(minute).append(":00"));
        }
    }

    public void setTime(View v) {
        DialogFragment newFragment = new TimePickerFragment();
        newFragment.show(getSupportFragmentManager(), "timePicker");
    }

    public class DateClosingPickerFragment extends DialogFragment
            implements DatePickerDialog.OnDateSetListener {

        @Override
        public Dialog onCreateDialog(Bundle savedInstanceState) {
            // Use the current date as the default date in the picker
            final Calendar c = Calendar.getInstance();
            int year = c.get(Calendar.YEAR);
            int month = c.get(Calendar.MONTH);
            int day = c.get(Calendar.DAY_OF_MONTH);

            DatePickerDialog dialog = new DatePickerDialog(getActivity(), this, year, month, day);
            Date date = new Date();
            date.setTime(date.getTime() - 1000);
            dialog.getDatePicker().setMinDate(date.getTime());
            Date date1 = new Date();
            date1.setTime(date.getTime() + 172800000);
            dialog.getDatePicker().setMaxDate(date1.getTime());

            return dialog;
        }

        public void onDateSet(DatePicker view, int year, int month, int day) {
            tvDate = (TextView) findViewById(R.id.tvCreateGroupDateClosing);

            tvDate.setText(new StringBuilder()
                    // Month is 0 based, just add 1
                    .append(year).append("-").append(month + 1).append("-")
                    .append(day));
        }
    }

    public void setDateClosing(View v) {
        DialogFragment newFragment = new DateClosingPickerFragment();
        newFragment.show(getSupportFragmentManager(), "datePicker");
    }

    public class TimeClosingPickerFragment extends DialogFragment
            implements TimePickerDialog.OnTimeSetListener {

        @Override
        public Dialog onCreateDialog(Bundle savedInstanceState) {
            // Use the current time as the default values for the picker
            final Calendar c = Calendar.getInstance();
            int hour = c.get(Calendar.HOUR_OF_DAY);
            int minute = c.get(Calendar.MINUTE);

            // Create a new instance of TimePickerDialog and return it
            return new TimePickerDialog(getActivity(), this, hour, minute,
                    DateFormat.is24HourFormat(getActivity()));
        }

        public void onTimeSet(TimePicker view, int hourOfDay, int minute) {
            tvTime = (TextView) findViewById(R.id.tvCreateGroupTimeClosing);

            tvTime.setText(new StringBuilder().append(hourOfDay).append(":").append(minute).append(":00"));
        }
    }

    public void setTimeClosing(View v) {
        DialogFragment newFragment = new TimeClosingPickerFragment();
        newFragment.show(getSupportFragmentManager(), "timePicker");
    }

    public void cancel(View v) {
        setResult(RESULT_CANCELED);
        finish();
    }

    public void post(View v) {
        EditText etGroupname = (EditText) findViewById(R.id.etCreateGroupGroupname);
        EditText etPrice = (EditText) findViewById(R.id.etCreateGroupPrice);
        tvDate = (TextView) findViewById(R.id.tvCreateGroupDate);
        tvTime = (TextView) findViewById(R.id.tvCreateGroupTime);
        tvDateClosing = (TextView) findViewById(R.id.tvCreateGroupDateClosing);
        tvTimeClosing = (TextView) findViewById(R.id.tvCreateGroupTimeClosing);

        String groupname = etGroupname.getText().toString();
        String price = etPrice.getText().toString();
        String endingTimestamp = tvDate.getText().toString() + " " + tvTime.getText().toString();
        String closingTimestamp = tvDateClosing.getText().toString() + " " + tvTimeClosing.getText().toString();

        (new CreateGroupTask()).execute(groupname, price, endingTimestamp, closingTimestamp);
    }

    private void showGroups(Group result) {
        String json = new Gson().toJson(result);
        Bundle data = new Bundle();
        data.putString("json-group", json);
        Intent intent = new Intent();
        intent.putExtras(data);
        setResult(RESULT_OK, intent);
        finish();
    }

}