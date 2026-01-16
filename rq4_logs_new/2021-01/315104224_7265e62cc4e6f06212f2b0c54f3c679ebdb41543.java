package com.catering.constants;

import java.util.ArrayList;
import java.util.List;

public enum Day {

    MONDAY(1, "monday"),
    TUESDAY(2, "tuesday"),
    WEDNESDAY(3, "wednesday"),
    THURSDAY(4, "thursday"),
    FRIDAY(5, "friday"),
    SATURDAY(6, "saturday"),
    SUNDAY(7, "sunday");

    private final int id;
    private final String name;

    Day(Integer id, String name) {
        this.id = id;
        this.name = name;
    }

    public static String getDayNameIfInputMatch(String input) {
        Day[] days = values();
        for (Day day : days) {
            if (day.getName().equalsIgnoreCase(input)) {
                return day.getName();
            }
        }
        return null;
    }

    public static List<String> getArrayDaysNames() {
        Day[] days = values();
        List<String> arrayDaysName = new ArrayList<>(days.length);
        for (Day day : days) {
            arrayDaysName.add(day.getName());
        }
        return arrayDaysName;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

}