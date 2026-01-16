package com.norsys.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DateUtil {

    /**
     * Construit un objet LocalDate à partir d'une date en string
     * @param date la date à parser
     * @return un objet LocalDate au format dd/MM/yyyy
     */
    public static LocalDate getLocalDate(String date) {
        return LocalDate.parse(date, DateTimeFormatter.ofPattern("dd/MM/yyyy"));
    }
}