package com.gowri.blitz.exceptions;

/*
 * @author NaveenWodeyar
 * @date 28-03-2025
 */

public class BlitzException extends RuntimeException {

    private int errorCode;  // Optional: You can provide an error code for more specific error handling
    private String details; // Optional: You can provide extra details specific to the error

    // Default constructor
    public BlitzException() {
        super();
    }

    // Constructor with message
    public BlitzException(String message) {
        super(message);
    }

    // Constructor with message and cause
    public BlitzException(String message, Throwable cause) {
        super(message, cause);
    }

    // Constructor with cause
    public BlitzException(Throwable cause) {
        super(cause);
    }

    // Constructor with message, cause, errorCode, and additional details
    public BlitzException(String message, Throwable cause, int errorCode, String details) {
        super(message, cause);
        this.errorCode = errorCode;
        this.details = details;
    }

    // Getter for errorCode
    public int getErrorCode() {
        return errorCode;
    }

    // Setter for errorCode
    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    // Getter for details
    public String getDetails() {
        return details;
    }

    // Setter for details
    public void setDetails(String details) {
        this.details = details;
    }

    // Optionally, override toString for custom exception printing
    @Override
    public String toString() {
        return "BlitzException{" +
                "message='" + getMessage() + '\'' +
                ", errorCode=" + errorCode +
                ", details='" + details + '\'' +
                '}';
    }
}