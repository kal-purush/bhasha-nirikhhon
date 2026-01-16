package com.smapley.powerwork.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 时间转换工具类
 * Created by smapley on 15/11/19.
 */
public class DateUtil {

    public static String formatDate = "yyyy-MM-dd";
    public static String formatDateAndTime = "yyyy-MM-dd HH:mm";

    /**
     * 将String转换成long
     */
    public static long getDateLong(String date, String format) {
        try {
            SimpleDateFormat sf = new SimpleDateFormat(format);
            return sf.parse(date).getTime();

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0l;
    }

    /**
     * 将long转换成String
     */
    public static String getDateString(long date, String format) {
        SimpleDateFormat sf = new SimpleDateFormat(format);
        return sf.format(date);
    }
}