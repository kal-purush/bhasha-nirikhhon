package com.herokuapp.exceptions;

public class ComparisonException extends AssertionError {

    public ComparisonException(String message, Throwable cause) {
        super(message, cause);
    }
}