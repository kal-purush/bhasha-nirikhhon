package properties;

import java.util.Calendar;

import static java.util.Calendar.*;

public class Problem1Calendar {

    public static void main(String[] args) {
        int count = 0;
        for (int year = 1901; year <= 2000; year++) {
            for (int month = JANUARY; month <= DECEMBER; month++) {
                if (fastDayOfWeek(year, month, 1) == SUNDAY) { count++;}
            }
        }
        System.out.printf("%s Sundays are 1st day of month in 21th Century%n", count);
    }

    static int dayOfWeek(int year, int month, int dayOfMonth) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(YEAR, year);
        calendar.set(MONTH, month);
        calendar.set(DAY_OF_MONTH, dayOfMonth);
        return calendar.get(DAY_OF_WEEK);
    }

    //https://www.quora.com/How-does-Tomohiko-Sakamotos-Algorithm-work
    static int monthOffset[] = {0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4};
    static int fastDayOfWeek(int year, int month, int dayOfMonth) {
        year -= month < 2 ? 1 : 0;
        return 1 + (year + year / 4 - year / 100 + year / 400 + monthOffset[month] + dayOfMonth) % 7;
    }

}