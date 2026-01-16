package com.interoffice.account.application.exception;

public class NotEqualPasswordException extends RuntimeException {
    public NotEqualPasswordException() {
        super("password that you input is not equal your account's password");
    }
}