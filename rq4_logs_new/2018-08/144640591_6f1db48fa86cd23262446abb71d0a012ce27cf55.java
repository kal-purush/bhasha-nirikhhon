package com.aaron.kata.babysitter;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.text.SimpleDateFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HoursCalculationServiceImplTest {

    private static final String HOURS_FORMAT = "hh:mma";
    private SimpleDateFormat timeFormatForTest;
    @Autowired
    private HoursCalculationService tested;

    @Before
    public void setUp() throws Exception {
        timeFormatForTest = new SimpleDateFormat(HOURS_FORMAT);
    }

    @Test
    public void testOnlyPriorToBedTime() throws Exception {
        assertEquals(
                "Calculated salary was incorrect",
                "36.00",
                tested.calculateSalaryEarned(
                        timeFormatForTest.parse("05:00pm"),
                        timeFormatForTest.parse("08:00pm"))
        );
    }

    @Test
    public void testAfterBedTimeAndBeforeMidnight() throws Exception {
        assertEquals(
                "Calculated salary was incorrect",
                "24.00",
                tested.calculateSalaryEarned(
                        timeFormatForTest.parse("08:00pm"),
                        timeFormatForTest.parse("12:00am"))
        );
    }

    @Test
    public void testOnlyAfterMidnight() throws Exception {
        assertEquals(
                "Calculated salary was incorrect",
                "64.00",
                tested.calculateSalaryEarned(
                        timeFormatForTest.parse("12:00am"),
                        timeFormatForTest.parse("4:00am"))
        );
    }

    @Test(expected = RuntimeException.class)
    public void testTooEarlyBefore5PM() throws Exception {
        tested.calculateSalaryEarned(timeFormatForTest.parse("3:00pm"),
                timeFormatForTest.parse("12:00am"));
        fail("Did not catch throw/catch expected exception");
    }

    @Test(expected = RuntimeException.class)
    public void tooLateAfter4AM() throws Exception {
        tested.calculateSalaryEarned(timeFormatForTest.parse("5:00pm"),
                timeFormatForTest.parse("5:00am"));
    }

    @Test
    public void testBeforeBedTimeUntilMidnight() throws Exception {
        assertEquals(
                "Calculated salary was incorrect",
                "60.00",
                tested.calculateSalaryEarned(
                        timeFormatForTest.parse("05:00pm"),
                        timeFormatForTest.parse("12:00am"))
        );
    }

    @Test
    public void testMaxTimeWorked() throws Exception {
        assertEquals(
                "Calculated salary was incorrect",
                "124.00",
                tested.calculateSalaryEarned(
                        timeFormatForTest.parse("5:00pm"),
                        timeFormatForTest.parse("4:00am"))
        );
    }

    @Test
    public void testAfterBedTimeUntilEnd() throws Exception {
        assertEquals(
                "Calculated salary was incorrect",
                "88.00",
                tested.calculateSalaryEarned(
                        timeFormatForTest.parse("08:00pm"),
                        timeFormatForTest.parse("4:00am"))
        );
    }


}