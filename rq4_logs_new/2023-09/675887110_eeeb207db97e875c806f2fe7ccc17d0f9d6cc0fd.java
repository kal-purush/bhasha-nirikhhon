package exceptions;

public class InvalidCredentialsException extends Exception {
    // Private Attributes ---------------------------------------------------------------
    private final String message;

    // Constructor ----------------------------------------------------------------------
    public InvalidCredentialsException(String message) {
        super(message);
        this.message = message;
    }

    // Getters --------------------------------------------------------------------------
    public String getMessage() {
        return message;
    }
}