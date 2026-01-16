package org.pesmypetcare.webservice.error;

/**
 * @author Marc Simó
 */
public class InvalidOperationException extends Exception {
    private final String errorCode;

    /**
     * Creates an invalid operation exception with the specified error code and detailed message.
     * @param errorCode The error code for the exception
     * @param detailedMessage A clarifying description of the error
     */
    public InvalidOperationException(String errorCode, String detailedMessage) {
        super(detailedMessage);
        this.errorCode = errorCode;
    }

    /**
     * Gets the error code for the exception.
     * @return The error code for the exception
     */
    public String getErrorCode() {
        return errorCode;
    }
}