package org.example.JavaTaigaCode.models;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class BurndownChartDTO {
    private LocalDate date;
    private Double value;

    public LocalDate getDate() {
        return date;
    }

    public void setDate(String date) {
        String datePattern = "MM-dd-yyyy";
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(datePattern);
        this.date = LocalDate.parse(date, dateFormatter);
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public BurndownChartDTO() {
    }

//    public BurndownChartDTO(String date, Double value) {
//        String datePattern = "MM-dd-yyyy";
//        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(datePattern);
//        this.date = LocalDate.parse(date, dateFormatter);
//        this.value = value;
//    }

    public BurndownChartDTO(LocalDate date, Double value) {
        this.date = date;
        this.value = value;
    }
}