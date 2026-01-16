package com.aaron.kata.babysitter;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class InputHoursValidator implements ConstraintValidator<InputHoursConstraint, DateTime> {
    private static final DateTimeFormatter HOURS_TIME_FORMAT = DateTimeFormat.forPattern("hh:mma");
    private static final String EARLIEST_START_TIME = "5:00pm";
    private static final String LATEST_END_TIME = "4:00am";

    @Override
    public void initialize(InputHoursConstraint constraintAnnotation) {

    }

    @Override
    public boolean isValid(DateTime value, ConstraintValidatorContext context) {
        return !value.isBefore(DateTime.parse(EARLIEST_START_TIME, HOURS_TIME_FORMAT))
                && !value.isAfter(DateTime.parse(LATEST_END_TIME, HOURS_TIME_FORMAT).plusDays(1));

    }


}