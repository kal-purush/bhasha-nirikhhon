package pl.dchruscinski.config.exception;

import java.time.LocalDateTime;

public class ErrorMessage {
    private final int statusCode;
    private final LocalDateTime timestamp;
    private final String message;
    private final String description;

    public ErrorMessage(int statusCode, LocalDateTime timestamp, String message, String description) {
        this.statusCode = statusCode;
        this.timestamp = timestamp;
        this.message = message;
        this.description = description;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public String getMessage() {
        return message;
    }

    public String getDescription() {
        return description;
    }
}