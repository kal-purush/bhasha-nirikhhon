package ru.otus.jdbc.mapper.exceptions;

public class InjectNewValueIdException extends JdbcMapperException {

    public InjectNewValueIdException() {
    }

    public InjectNewValueIdException(String message) {
        super(message);
    }

    public InjectNewValueIdException(String message, Throwable cause) {
        super(message, cause);
    }

    public InjectNewValueIdException(Throwable cause) {
        super(cause);
    }

    public InjectNewValueIdException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}