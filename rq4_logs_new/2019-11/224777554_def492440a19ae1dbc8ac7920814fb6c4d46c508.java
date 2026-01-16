package com.sec.news.model;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateTime {

    public static String date(Date date){

        DateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        String endTimee = format.format(date);

        return  endTimee;
    }
}