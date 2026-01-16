package com.driver.exceptions;

public class CannotMakeReservationException extends RuntimeException {

    public CannotMakeReservationException(String m){
        super(m);
    }
}