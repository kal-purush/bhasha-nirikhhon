package com.zjl.crm.exception;

public class CheckCodeException extends LoginException{
    public CheckCodeException() {
    }

    public CheckCodeException(String message) {
        super(message);
    }
}