package com.reservibe.domain.entity.restaurant;

import com.reservibe.infra.model.restaurant.OpeningHoursModel;

import java.time.DayOfWeek;
import java.time.LocalDateTime;

public class OpeningHours {

    private final DayOfWeek dayOfWeek;
    private final LocalDateTime start;
    private final LocalDateTime end;

    public OpeningHours(DayOfWeek dayOfWeek, LocalDateTime start, LocalDateTime end) {
        this.dayOfWeek = dayOfWeek;
        this.start = start;
        this.end = end;
    }

    public OpeningHours(OpeningHoursModel openingHoursModel) {
        this(openingHoursModel.getDayOfWeek(), openingHoursModel.getStart(), openingHoursModel.getEnd());
    }

    public LocalDateTime getStart() {
        return start;
    }

    public LocalDateTime getEnd() {
        return end;
    }

    public DayOfWeek getDayOfWeek() {
        return dayOfWeek;
    }

    public boolean isOpen(LocalDateTime dateTime) {
        return dateTime.getDayOfWeek().equals(dayOfWeek) && dateTime.isAfter(start) && dateTime.isBefore(end);
    }

    public boolean isClosed(LocalDateTime dateTime) {
        return !isOpen(dateTime);
    }

    public boolean isBeforeOpening(LocalDateTime dateTime) {
        return dateTime.getDayOfWeek().equals(dayOfWeek) && dateTime.isBefore(start);
    }

    public boolean isAfterClosing(LocalDateTime dateTime) {
        return dateTime.getDayOfWeek().equals(dayOfWeek) && dateTime.isAfter(end);
    }

    public boolean isBeforeOrAfterOpening(LocalDateTime dateTime) {
        return isBeforeOpening(dateTime) || isAfterClosing(dateTime);
    }

    public boolean isBeforeOrAfterClosing(LocalDateTime dateTime) {
        return isBeforeOpening(dateTime) || isAfterClosing(dateTime);
    }

    public boolean isBeforeOrAfterOpeningOrClosing(LocalDateTime dateTime) {
        return isBeforeOpening(dateTime) || isAfterClosing(dateTime);
    }

    public boolean isBeforeOrAfterOpeningAndClosing(LocalDateTime dateTime) {
        return isBeforeOpening(dateTime) || isAfterClosing(dateTime);
    }

    public boolean isBeforeOrAfterOpeningOrClosingOrBoth(LocalDateTime dateTime) {
        return isBeforeOpening(dateTime) || isAfterClosing(dateTime);
    }

    public boolean isBeforeOrAfterOpeningAndClosingOrBoth(LocalDateTime dateTime) {
        return isBeforeOpening(dateTime) || isAfterClosing(dateTime);
    }

    public boolean isBeforeOrAfterOpeningAndClosingOrBothOrNone(LocalDateTime dateTime) {
        return isBeforeOpening(dateTime) || isAfterClosing(dateTime);
    }

    public boolean isBeforeOrAfterOpeningAndClosingOrBothOrNoneOrAll(LocalDateTime dateTime) {
        return isBeforeOpening(dateTime) || isAfterClosing(dateTime);
    }
}