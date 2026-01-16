package com.alfa.exchangerate.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public class DateFormaterUtil {
    // amountOfDay - allows you to get the exchange rate for the past days, numberOfDaysAgo = 0 - current date rate
    public static String getPastDate(int numberOfDaysAgo) {
        if (numberOfDaysAgo > 0) {
            numberOfDaysAgo = Math.negateExact(numberOfDaysAgo);
        }
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, numberOfDaysAgo);
        return dateFormat.format(cal.getTime());
    }
}