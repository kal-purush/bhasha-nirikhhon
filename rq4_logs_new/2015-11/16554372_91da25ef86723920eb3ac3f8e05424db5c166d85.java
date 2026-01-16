package com.joymeter.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by hramovecchi on 09/11/2015.
 */
public class DateUtils {

    private static DateUtils instance = null;
    private static final String stringDateFormat = "dd/MM/yyyy";
    private static SimpleDateFormat dateFormat = null;

    private DateUtils(){

    }

    public static DateUtils getInstance(){
        if (instance == null){
            instance = new DateUtils();
            dateFormat = new SimpleDateFormat(stringDateFormat);
        }
        return instance;
    }

    public String getFormatedDate(Date date){
        return dateFormat.format(date);
    }

    public Date getDate(long milliseconds){
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(milliseconds);
        return c.getTime();
    }
}