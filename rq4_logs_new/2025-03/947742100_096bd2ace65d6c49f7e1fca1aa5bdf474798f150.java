package exception;

public class NoValidGradeForUrlException extends RuntimeException {
    public NoValidGradeForUrlException() {
    }

    public NoValidGradeForUrlException(String message) {
        super(message);
    }

    public NoValidGradeForUrlException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoValidGradeForUrlException(Throwable cause) {
        super(cause);
    }

    public NoValidGradeForUrlException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}