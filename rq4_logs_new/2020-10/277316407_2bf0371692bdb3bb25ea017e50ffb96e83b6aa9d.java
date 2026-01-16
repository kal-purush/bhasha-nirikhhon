package ru.otus.db.exceptions;

public class MigrateException extends RuntimeException {

    public MigrateException() {
    }

    public MigrateException(String message) {
        super(message);
    }

    public MigrateException(String message, Throwable cause) {
        super(message, cause);
    }

    public MigrateException(Throwable cause) {
        super(cause);
    }

    public MigrateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}