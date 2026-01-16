package com.roundsToThree.Representations;

import com.roundsToThree.DataProcessing.ByteUtils;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

public class DateTimeRepresentation {
    /*
        Data is stored like this
        YYYYMMDDHHMMSS.FFFFFF&ZZXX
        YYYY = Year
        (Everything else is optional - Stop reading on NULL)
        MM = Month
        DD = Day
        HH = Hour
        MM = Minute
        SS = Seconds
        FFFFFF = Fraction of second (down to 1 millionth)

        // Note:
        while you terminate reading on the first NULL, do keep in mind that
        UTC offset wont necessarily terminate there as well:
        202209+1100 is valid even though every other parameter is missing

        &  = +/- For UTC Offset
        ZZ = UTC Offset Hours
        XX = UTC Offset Minutes
    */

    // Class specific variables
    public ZonedDateTime dateTime;

    // Converts raw DateTime value into a easier to use format
    public DateTimeRepresentation(byte[] data) {
        // Basic error checking
        if (data == null || data.length < 4)
            return;

        int year = 0;
        int month = 0;
        int day = 0;
        int hour = 0;
        int minutes = 0;
        int seconds = 0;
        int microseconds = 0;
        int hourOffset = 0;
        int minuteOffset = 0;

        byte[] dateData = new byte[0];
        byte[] offsetData = new byte[0];
        // Start by looking for UTC offset
        for (int i = 0; i < data.length; i++)
            if (data[i] == '-' || data[i] == '+') {
                dateData = Arrays.copyOfRange(data, 0, i);
                offsetData = Arrays.copyOfRange(data, i, data.length);
                break;
            }
        // If no offset was found then use the entire data as dateData
        if (dateData.length == 0)
            dateData = data;

        // Read the year
        year = (int) ByteUtils.convertCharactersToLong(Arrays.copyOfRange(dateData, 0, 4));

        // Read month
        if (dateData.length >= 6)
            month = (int) ByteUtils.convertCharactersToLong(Arrays.copyOfRange(dateData, 4, 6));
        // Read day
        if (dateData.length >= 8)
            day = (int) ByteUtils.convertCharactersToLong(Arrays.copyOfRange(dateData, 6, 8));
        // Read hour
        if (dateData.length >= 10)
            hour = (int) ByteUtils.convertCharactersToLong(Arrays.copyOfRange(dateData, 8, 10));
        // Read minutes
        if (dateData.length >= 12)
            minutes = (int) ByteUtils.convertCharactersToLong(Arrays.copyOfRange(dateData, 10, 12));
        // Read seconds
        if (dateData.length >= 14)
            seconds = (int) ByteUtils.convertCharactersToLong(Arrays.copyOfRange(dateData, 12, 14));
        // Read microseconds
        if (dateData.length >= 21)
            // Exclude decimal point
            microseconds = (int) ByteUtils.convertCharactersToLong(Arrays.copyOfRange(dateData, 15, 21));

        // Now find utc offset
        if (offsetData.length >= 3)
            hourOffset = (int) ByteUtils.convertCharactersToLong(Arrays.copyOfRange(offsetData, 0, 3));

        if (offsetData.length >= 5)
            minuteOffset = (int) ByteUtils.convertCharactersToLong(Arrays.copyOfRange(offsetData, 3, 5));

        dateTime = ZonedDateTime.of(
                year,
                Math.max(month, 1),
                Math.max(day, 1),
                hour,
                minutes,
                seconds,
                microseconds * 1000,
                ZoneId.ofOffset("UTC", ZoneOffset.ofHoursMinutes(hourOffset, minuteOffset))
        );
    }

    public String toString() {
        return dateTime.format(DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm:ss.SSSSSSa ZZZZ"));
    }

}