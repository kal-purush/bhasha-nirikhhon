package com.aaron.kata.babysitter;

public enum ErrorMessages {
    OUTSIDE_INPUT_WINDOW("Time input must be between 5:00PM and 4:00AM"),
    NULL_INPUT("Time input cannot be blank"),
    END_TIME_NOT_LAST("End time must be later than start time");

    private String message;

    private ErrorMessages(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}