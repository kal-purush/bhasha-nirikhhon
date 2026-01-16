package ru.otus.jdbc.mapper.exceptions;

public class ReadEntityException extends RuntimeException {

    public ReadEntityException() {
    }

    public ReadEntityException(String message) {
        super(message);
    }

    public ReadEntityException(String message, Throwable cause) {
        super(message, cause);
    }

    public ReadEntityException(Throwable cause) {
        super(cause);
    }

    public ReadEntityException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}