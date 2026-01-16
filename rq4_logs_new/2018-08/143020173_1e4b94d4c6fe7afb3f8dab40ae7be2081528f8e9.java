package ya.co.yandex_finance.ui.fragment.transactions;

import android.content.Context;
import android.content.res.TypedArray;
import android.graphics.Color;
import android.support.annotation.Nullable;
import android.support.v4.app.FragmentManager;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.AppCompatSpinner;
import android.text.TextUtils.TruncateAt;
import android.util.AttributeSet;
import android.util.Log;
import android.view.Gravity;
import android.view.LayoutInflater;
import android.view.View;
import android.widget.AdapterView;
import android.widget.AdapterView.OnItemSelectedListener;
import android.widget.ArrayAdapter;
import android.widget.LinearLayout;
import android.widget.TextView;

import com.twigsntwines.daterangepicker.DatePickerDialog;
import com.twigsntwines.daterangepicker.DateRangePickedListener;

import java.util.Calendar;

import ya.co.yandex_finance.R;

public class DateRangePickerSpinner extends LinearLayout {
    private static final String TAG = DateRangePickerSpinner.class.getSimpleName();
    TextView spinnerText;
    CustomSpinner dateSpinner;

    private Calendar cal;

    private AppCompatActivity activity;

    private DateRangePickedListener listener;
    private final String[] dateSpinnerArray;
    private String today;
    private String week;
    private String month;
    private String custom;

    /**
     * Constructor
     **/
    public DateRangePickerSpinner(Context context, @Nullable AttributeSet attrs) {
        super(context, attrs);
        activity = (AppCompatActivity) context;
        dateSpinnerArray = context.getResources().getStringArray(R.array.date_picker_categories);
        initDays();
        TypedArray typedArray = getContext().obtainStyledAttributes(attrs, R.styleable.DatePickerSpinner);
        String textViewString = typedArray.getString(R.styleable.DatePickerSpinner_spinnerText);
        typedArray.recycle();
        setOrientation(LinearLayout.HORIZONTAL);
        setGravity(Gravity.CENTER_VERTICAL);
        setPadding(8, 9, 8, 8);
        LayoutInflater layoutInflater = (LayoutInflater) context.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
        if (layoutInflater != null) {
            layoutInflater.inflate(R.layout.date_picker_spinner, this, true);
            spinnerText = (TextView) getChildAt(0);
            spinnerText.setText(textViewString);
            spinnerText.setEllipsize(TruncateAt.END);
            spinnerText.setText(R.string.spinnerText);
            spinnerText.setPadding(8, 8, 8, 8);
            spinnerText.setTextColor(Color.BLACK);
            spinnerText.setGravity(Gravity.START | Gravity.CENTER);
            spinnerText.setMaxLines(1);
            dateSpinner = (CustomSpinner) getChildAt(1);
            initializeSpinner();
        }

        cal = Calendar.getInstance();
    }

    /**
     * Set Listener for DateRangePicked Interface
     **/
    public void setDateRangePickedListener(DateRangePickedListener listener) {
        this.listener = listener;
    }

    /**
     * Set Text for spinnerText programmatically
     **/
    public void setSpinnerText(String text) {
        spinnerText.setText(text);
    }

    /**
     * Set Visibility for spinnerText programmatically
     **/
    public void setSpinnerTextVisibility(boolean visibility) {
        spinnerText.setVisibility(visibility ? View.VISIBLE : View.GONE);
    }

    /**
     * Returns Text assigned to spinnerText
     **/
    public String getSpinnerText() {
        return spinnerText.getText().toString();
    }

    /**
     * Returns spinnerText
     **/
    public TextView getSpinnerTextView() {
        return spinnerText;
    }

    private void initDays() {
        today = dateSpinnerArray[0];
        week = dateSpinnerArray[1];
        month = dateSpinnerArray[2];
    }

    /**
     * Initializes spinner adapter and list values
     **/
    private void initializeSpinner() {
        ArrayAdapter<String> adapter = new ArrayAdapter<>(getContext(), android.R.layout.simple_spinner_dropdown_item, dateSpinnerArray);
        dateSpinner.setAdapter(adapter);
        dateSpinner.setOnItemSelectedListener(new OnItemSelectedListener() {
            @Override
            public void onItemSelected(AdapterView<?> adapterView, View view, int i, long l) {
                String selectedItem = dateSpinnerArray[i];
                Log.e(TAG, "Selected Item -> " + selectedItem);
                if (!selectedItem.equals(dateSpinnerArray[dateSpinnerArray.length - 1])) {
                    if (listener != null)
                        listener.OnDateRangePicked(getFromDate(selectedItem), getToDate());
                } else {
                    FragmentManager fragmentManager = activity.getSupportFragmentManager();
                    DatePickerDialog datePickerDialog = DatePickerDialog.newInstance();
                    datePickerDialog.setOnDateRangePickedListener(listener);
                    datePickerDialog.show(fragmentManager, TAG);
                }
            }

            @Override
            public void onNothingSelected(AdapterView<?> adapterView) {

            }
        });
    }

    /**
     * Returns Calendar object for FromDate based on selectedItem
     **/
    private Calendar getFromDate(String selectedItem) {
        cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.clear(Calendar.MINUTE);
        cal.clear(Calendar.SECOND);
        if (selectedItem.equals(week)) {
            cal.set(Calendar.DAY_OF_WEEK, 1);
        } else if (selectedItem.equals(month)) {
            cal.set(Calendar.DAY_OF_MONTH, 1);
        }

        return cal;
    }

    /**
     * Returns Calendar object for ToDate
     **/
    private Calendar getToDate() {
        cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        return cal;
    }

    public static class CustomSpinner extends AppCompatSpinner {

        public CustomSpinner(Context context) {
            super(context);
        }

        public CustomSpinner(Context context, AttributeSet attrs) {
            super(context, attrs);
        }

        public CustomSpinner(Context context, AttributeSet attrs, int defStyleAttr) {
            super(context, attrs, defStyleAttr);
        }

        @Override
        public void setSelection(int position) {
            boolean sameSelected = position == getSelectedItemPosition();
            super.setSelection(position);
            if (sameSelected) {
                if (getOnItemSelectedListener() != null)
                    getOnItemSelectedListener().onItemSelected(this, getSelectedView(), position, getSelectedItemId());
            }
        }

        @Override
        public void setSelection(int position, boolean animate) {
            boolean sameSelected = position == getSelectedItemPosition();
            super.setSelection(position, animate);
            if (sameSelected) {
                if (getOnItemSelectedListener() != null)
                    getOnItemSelectedListener().onItemSelected(this, getSelectedView(), position, getSelectedItemId());
            }
        }
    }
}