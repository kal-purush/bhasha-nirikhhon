package com.devsuda.lancasterprayertimes;

import android.database.Cursor;
import android.graphics.Color;
import android.widget.TableLayout;
import android.widget.TextView;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Abubakr on 07/06/2017.
 */

public class DisplayPrayerTimes {

    private static final int igamaCountDownId = 1;
    private static final int azanCountDownId = 2;
    private static final int maghribCountDownId = 3;
    private MainActivity mainActivity;

    private long fajr_in_ms;
    private long zhor_in_ms;
    private long asor_in_ms;
    private long magrib_in_ms;
    private long isha_in_ms;

    private long nextPrayerTimeInMs = 0;
    private long nextPrayerAzanTimeInMs = 0;

    private long currentTimeInMillis;
    private long timeToNextIgamaInMillis;
    private long timeToNextAzanInMillis;

    private long fajrAzan_in_ms;
    private long zhorAzan_in_ms;
    private long asorAzan_in_ms;
    private long ishaAzan_in_ms;

    private Date fajr_time;
    private Date zhor_time;
    private Date asor_time;
    private Date magrib_time;
    private Date isha_time;

    private Date fajrAzan_time;
    private Date zhorAzan_time;
    private Date asorAzan_time;
    private Date ishaAzan_time;

    private Date current_time;
    private String currentDate;
    private Date tomorrow_date = null;
    private Calendar calendar_1;
    private Calendar calendar_2;
    private Calendar cal1;
    private String nextDay;

    private DateTimeAdaptor dateTimeAdaptor;
    private DisplayCountdowns displayCountdowns;

    private static TextView DuaaLabel;

    private static TextView dateNameTV;
    private static TextView gregorianTimeTV;

    private static TextView shehriLblTV;
    private static TextView fajrAzanTV;
    private static TextView fajrIgamaTV;
    private static TextView sunriseTV;
    private static TableLayout fajrBgTl;

    private static TextView zhorAzanTV;
    private static TextView zhorIgamaTV;
    private static TableLayout zhorBgTL;

    private static TextView asorAzanHanafiTV;
    private static TextView asorIgamaTV;
    private static TableLayout asorBgTL;

    private static TextView magribIgamaTV;
    private static TableLayout magribBgTL;

    private static TextView ishaAzanTV;
    private static TextView ishaIgamaTV;
    private static TableLayout ishaBgTL;

    public DisplayPrayerTimes(MainActivity _mainActivity) {
        this.mainActivity = _mainActivity;
        this.dateTimeAdaptor = new DateTimeAdaptor();
        this.displayCountdowns = new DisplayCountdowns(_mainActivity);
        this.current_time = new Date();
        this.currentDate = current_time.toString();
        this.calendar_1 = Calendar.getInstance();
        this.calendar_2 = Calendar.getInstance();
        this.cal1 = Calendar.getInstance();

        this.DuaaLabel = (TextView) _mainActivity.findViewById(R.id.DuaaLabel);

        this.dateNameTV = (TextView) _mainActivity.findViewById(R.id.dateNameTV);
        this.gregorianTimeTV = (TextView) _mainActivity.findViewById(R.id.gregorianTimeTV);

        this.shehriLblTV = (TextView) _mainActivity.findViewById(R.id.shehriLblTV);
        this.fajrAzanTV = (TextView) _mainActivity.findViewById(R.id.fajrAzanTV);
        this.fajrIgamaTV = (TextView) _mainActivity.findViewById(R.id.fajrIgamaTV);
        this.sunriseTV = (TextView) _mainActivity.findViewById(R.id.sunriseTV);
        this.fajrBgTl = (TableLayout) _mainActivity.findViewById(R.id.fajrBgTL);

        this.zhorAzanTV = (TextView) _mainActivity.findViewById(R.id.zhorAzanTV);
        this.zhorIgamaTV = (TextView) _mainActivity.findViewById(R.id.zhorIgamaTV);
        this.zhorBgTL = (TableLayout) _mainActivity.findViewById(R.id.zhorBgTL);

        this.asorAzanHanafiTV = (TextView) _mainActivity.findViewById(R.id.asorAzanHanafiTV);
        this.asorIgamaTV = (TextView) _mainActivity.findViewById(R.id.asorIgamaTV);
        this.asorBgTL = (TableLayout) _mainActivity.findViewById(R.id.asorBgTL);

        this.magribIgamaTV = (TextView) _mainActivity.findViewById(R.id.magribIgamaTV);
        this.magribBgTL = (TableLayout) _mainActivity.findViewById(R.id.magribBgTL);

        this.ishaAzanTV = (TextView) _mainActivity.findViewById(R.id.ishaAzanTV);
        this.ishaIgamaTV = (TextView) _mainActivity.findViewById(R.id.ishaIgamaTV);
        this.ishaBgTL = (TableLayout) _mainActivity.findViewById(R.id.ishaBgTL);
    }

    public void get_time_diff(Cursor cursor, DatabaseAdaptor databaseAdaptor) {

        displayBasicInfo(cursor);

        fajr_in_ms = dateTimeAdaptor.convertToMillis(cursor, 1, null);
        zhor_in_ms = dateTimeAdaptor.convertToMillis(cursor, 2, null);
        asor_in_ms = dateTimeAdaptor.convertToMillis(cursor, 3, null);
        magrib_in_ms = dateTimeAdaptor.convertToMillis(cursor, 4, null);
        isha_in_ms = dateTimeAdaptor.convertToMillis(cursor, 5, null);

        fajrAzan_in_ms = dateTimeAdaptor.convertToMillis(cursor, 7, null);
        zhorAzan_in_ms = dateTimeAdaptor.convertToMillis(cursor, 8, null);
        asorAzan_in_ms = dateTimeAdaptor.convertToMillis(cursor, 9, null);
        ishaAzan_in_ms = dateTimeAdaptor.convertToMillis(cursor, 10, null);

        fajr_time = new Date(fajr_in_ms);
        zhor_time = new Date(zhor_in_ms);
        asor_time = new Date(asor_in_ms);
        magrib_time = new Date(magrib_in_ms);
        isha_time = new Date(isha_in_ms);

        fajrAzan_time = new Date(fajrAzan_in_ms);
        zhorAzan_time = new Date(zhorAzan_in_ms);
        asorAzan_time = new Date(asorAzan_in_ms);
        ishaAzan_time = new Date(ishaAzan_in_ms);

        // ====================================================

        if (current_time.before(fajrAzan_time)) {
            nextPrayerAzanTimeInMs = fajrAzan_in_ms;
        } else if (current_time.before(zhorAzan_time)
                && current_time.after(fajr_time)) {
            nextPrayerAzanTimeInMs = zhorAzan_in_ms;
        } else if (current_time.before(asorAzan_time)
                && current_time.after(zhor_time)) {
            nextPrayerAzanTimeInMs = asorAzan_in_ms;
        } else if (current_time.before(magrib_time)
                && current_time.after(asor_time)) {
            nextPrayerAzanTimeInMs = magrib_in_ms;
        } else if (current_time.before(ishaAzan_time)
                && current_time.after(magrib_time)) {
            nextPrayerAzanTimeInMs = ishaAzan_in_ms;
        } else if (current_time.after(isha_time)) {
            // initialize the calendar with current day
            try {
                calendar_1.setTime(dateTimeAdaptor.dateformat_2().parse(currentDate));
            } catch (ParseException e) {
                e.printStackTrace();
            }

            // go to next day in the calendar
            calendar_1.add(Calendar.DATE, 1);

            // Returns the time of this Calendar as a DateTimeAdaptor object.
            nextDay = dateTimeAdaptor.dateformat_2().format(calendar_1.getTime());

            // parse nextDay with format_2 to get the right "date" syntax
            try {
                tomorrow_date = dateTimeAdaptor.dateformat_2().parse(nextDay);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            // set tomorrow's date as toda's date in the calendar
            calendar_2.setTime(tomorrow_date);

            // Query the database
            int dayOfMonth = calendar_2.get(Calendar.DAY_OF_MONTH);
            Cursor a = databaseAdaptor.getPrayersTimeOnDay(dayOfMonth);

            fajrAzan_in_ms = dateTimeAdaptor.convertToMillis(a, 7, null);
            nextPrayerAzanTimeInMs = fajrAzan_in_ms;
        }

        // ====================================================

        if (current_time.before(fajr_time)) {
            nextPrayerTimeInMs = fajr_in_ms;
            fajrBgTl.setBackgroundColor(Color.parseColor("#f1c40f"));
        } else if (current_time.before(zhor_time)
                && current_time.after(fajr_time)) {
            if (dateTimeAdaptor.isFriday() == true) {
                zhorBgTL.setBackgroundColor(Color.parseColor("#f1c40f"));
                nextPrayerTimeInMs = zhor_in_ms;
            } else {
                nextPrayerTimeInMs = zhor_in_ms;
                zhorBgTL.setBackgroundColor(Color.parseColor("#f1c40f"));
            }
        } else if (current_time.before(asor_time)
                && current_time.after(zhor_time)) {
            nextPrayerTimeInMs = asor_in_ms;
            asorBgTL.setBackgroundColor(Color.parseColor("#f1c40f"));
        } else if (current_time.before(magrib_time)
                && current_time.after(asor_time)) {
            nextPrayerTimeInMs = magrib_in_ms;
            magribBgTL.setBackgroundColor(Color.parseColor("#f1c40f"));
        } else if (current_time.before(isha_time)
                && current_time.after(magrib_time)) {
            nextPrayerTimeInMs = isha_in_ms;
            ishaBgTL.setBackgroundColor(Color.parseColor("#f1c40f"));
        } else if (current_time.after(isha_time)) {

            // initialize the calendar with current day
            try {
                calendar_1.setTime(dateTimeAdaptor.dateformat_2().parse(currentDate));
            } catch (ParseException e) {
                e.printStackTrace();
            }

            // go to next day in the calendar
            calendar_1.add(Calendar.DATE, 1);

            // Returns the time of this Calendar as a DateTimeAdaptor object.
            nextDay = dateTimeAdaptor.dateformat_2().format(calendar_1.getTime());

            // parse nextDay with format_2 to get the right "date" syntax
            try {
                tomorrow_date = dateTimeAdaptor.dateformat_2().parse(nextDay);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            // set tomorrow's date as toda's date in the calendar
            calendar_2.setTime(tomorrow_date);

            // Query the database
            int dayOfMonth = calendar_2.get(Calendar.DAY_OF_MONTH);
            Cursor b = databaseAdaptor.getPrayersTimeOnDay(dayOfMonth);

            // =======================================
            DuaaLabel.setText("(All times are NOW set for tomorrow)");

            fajrAzanTV.setText(b.getString(4));
            fajrIgamaTV.setText(b.getString(5));
            sunriseTV.setText(b.getString(6));

            zhorAzanTV.setText(b.getString(7));
            zhorIgamaTV.setText(b.getString(8));

            asorAzanHanafiTV.setText(b.getString(10));
            asorIgamaTV.setText(b.getString(11));

            magribIgamaTV.setText(b.getString(12));

            ishaAzanTV.setText(b.getString(13));
            ishaIgamaTV.setText(b.getString(14));

            fajr_in_ms = dateTimeAdaptor.convertToMillis(b, 1, null);
            asor_in_ms = dateTimeAdaptor.convertToMillis(b, 3, null);
            magrib_in_ms = dateTimeAdaptor.convertToMillis(b, 4, null);

            asor_time = new Date(asor_in_ms);
            magrib_time = new Date(magrib_in_ms);

            nextPrayerTimeInMs = fajr_in_ms;
            fajrBgTl.setBackgroundColor(Color.parseColor("#f1c40f"));

        }

        currentTimeInMillis = Calendar.getInstance().getTimeInMillis();
        timeToNextIgamaInMillis = nextPrayerTimeInMs - currentTimeInMillis;
        timeToNextAzanInMillis = nextPrayerAzanTimeInMs - currentTimeInMillis;

        if (current_time.before(magrib_time) && current_time.after(asor_time)) {
            displayCountdowns.countdown(timeToNextIgamaInMillis, igamaCountDownId);
            displayCountdowns.countdown(timeToNextIgamaInMillis, maghribCountDownId);
        } else {
            displayCountdowns.countdown(timeToNextIgamaInMillis, igamaCountDownId);
            displayCountdowns.countdown(timeToNextAzanInMillis, azanCountDownId);
        }
    }

    private void displayBasicInfo(Cursor cursor) {

        setDateNameTV(cal1.get(Calendar.DAY_OF_WEEK));

        dateTimeAdaptor.gregorianDateFormat().format(new Date());
        this.gregorianTimeTV = (TextView) mainActivity.findViewById(R.id.gregorianTimeTV);
        gregorianTimeTV.setText("test");

        shehriLblTV.setText(cursor.getString(3));
        fajrAzanTV.setText(cursor.getString(4));
        fajrIgamaTV.setText(cursor.getString(5));
        sunriseTV.setText(cursor.getString(6));

        zhorAzanTV.setText(cursor.getString(7));
        zhorIgamaTV.setText(cursor.getString(8));

        asorAzanHanafiTV.setText(cursor.getString(10));
        asorIgamaTV.setText(cursor.getString(11));

        magribIgamaTV.setText(cursor.getString(12));

        ishaAzanTV.setText(cursor.getString(13));
        ishaIgamaTV.setText(cursor.getString(14));
    }

    private void setDateNameTV(int currentDay) {
        switch (currentDay) {
            case Calendar.SATURDAY:
                dateNameTV.setText("Saturday");
                break;
            case Calendar.SUNDAY:
                dateNameTV.setText("Sunday");
                break;
            case Calendar.MONDAY:
                dateNameTV.setText("Monday");
                break;
            case Calendar.TUESDAY:
                dateNameTV.setText("Tuesday");
                break;
            case Calendar.WEDNESDAY:
                dateNameTV.setText("Wednesday");
                break;
            case Calendar.THURSDAY:
                dateNameTV.setText("Thursday");
                break;
            case Calendar.FRIDAY:
                dateNameTV.setText("Friday");
                break;
        }
    }

}