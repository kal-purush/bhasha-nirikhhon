package model;

import lombok.Getter;

import java.time.LocalDateTime;

@Getter
public enum DateEnum {
    JANUARY(1, 31),
    FEBRUARY(2, 28),
    MARCH(3, 31),
    APRIL(4, 30),
    MAY(5, 31),
    JUNE(6, 30),
    JULY(7, 31),
    AUGUST(8, 31),
    SEPTEMBER(9, 30),
    OCTOBER(10, 31),
    NOVEMBER(11, 30),
    DECEMBER(12, 31);

    private final int month;
    private final int days;
    private static final LocalDateTime localDateTime = LocalDateTime.now();

    DateEnum(int month, int days) {
        this.month = month;
        this.days = days;
    }
    public Integer getToday() {
        return localDateTime.getDayOfMonth();
    }
    public Integer getMonthToday() {
        return localDateTime.getMonthValue();
    }
    public Integer getYearToday() {
        return localDateTime.getYear();
    }

}