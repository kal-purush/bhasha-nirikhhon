package com.milkmatters.honoursproject.milkmatters.model;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for the donation model class
 */
public class DonationTest {
    Donation donation;
    String date = "1-2-2016";
    String sqlDate = "01-02-2016";
    String analogueDate = "1 Feb 16";
    int quantity = 50;

    @Before
    public void setUp() throws Exception {
        donation = new Donation(date, quantity);
    }

    @Test
    public void getSQLDate() throws Exception {
        String[] dates = {"1-2-2016", "01-2-2016", "1-02-2016", "01-02-2016",
                "01-11-2016", "1-11-2016", "11-1-2016", "11-11-2016"};
        String[] sqlDates = {"01-02-2016", "01-02-2016", "01-02-2016", "01-02-2016",
                "01-11-2016", "01-11-2016", "11-01-2016", "11-11-2016"};
        for (int i = 0; i < dates.length; i++)
        {
            donation.setDate(dates[i]);
            assertEquals(sqlDates[i], donation.getSQLDate());
        }
    }

    @Test
    public void getAnalogueDate() throws Exception {
        String[] dates = {"1-2-2016", "01-2-2016", "1-02-2016", "01-02-2016",
                "01-11-2016", "1-11-2016", "11-1-2016", "11-11-2016"};
        String[] analogueDates = {"1 Feb 16", "01 Feb 16", "1 Feb 16", "01 Feb 16",
                "01 Nov 16", "1 Nov 16", "11 Jan 16", "11 Nov 16"};
        for (int i = 0; i < dates.length; i++)
        {
            donation.setDate(dates[i]);
            assertEquals(analogueDates[i], donation.getAnalogueDate());
        }
    }

    @Test
    public void increaseDonation() throws Exception {
        donation.increaseDonation(50);
        assertEquals(100, donation.getQuantity());
    }
}