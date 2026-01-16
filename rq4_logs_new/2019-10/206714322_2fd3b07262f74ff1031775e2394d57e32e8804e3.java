package com.newestuser.imen.bgdate;

import org.junit.Test;

import java.util.Calendar;
import java.util.Date;

import static org.junit.Assert.*;

public class BulgarianDateTest {

    @Test
    public void formatGregorianDatetoBulgarianDate() {
        Date gregorianDate = firstOfJanuary(2019);

        BulgarianDate bulgarianDate = new BulgarianDate(gregorianDate);
        String formatedDate = bulgarianDate.format(bulgarianDate);
        assertEquals(formatedDate, "10 Алем, 6787 Докс");
    }

    private Date firstOfJanuary(int year) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, Calendar.JANUARY);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        return calendar.getTime();
    }
}