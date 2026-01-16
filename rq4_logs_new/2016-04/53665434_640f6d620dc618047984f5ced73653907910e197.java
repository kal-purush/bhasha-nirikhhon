package ir.visoft.accounting.util;

import java.util.Calendar;
import java.util.Date;

/**
 * @author Amir
 */
public class DateUtil {


    public static Calendar dateToCalendar(Date date) {

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar;

    }

    public static String getDateAsString(Date date, String separator) {
        String dateAsString = "";
        Calendar calendar = dateToCalendar(date);
        //dateAsString = calendar.get(Calendar.YEAR) + separator + calendar.get(Calendar.MONTH) + separator + calendar.get(Calendar.DAY_OF_MONTH);
        int month = calendar.get(Calendar.MONTH) + 1;
        dateAsString = calendar.get(Calendar.DAY_OF_MONTH) + separator + month + separator + calendar.get(Calendar.YEAR);
        return dateAsString;
    }

    public static String getDateAsString(Date date) {
        return getDateAsString(date, "/");
    }

}