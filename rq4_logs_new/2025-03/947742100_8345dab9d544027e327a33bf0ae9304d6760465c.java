public class NoSuchCommandException extends RuntimeException {
    public NoSuchCommandException() {
    }

    public NoSuchCommandException(String message) {
        super(message);
    }

    public NoSuchCommandException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoSuchCommandException(Throwable cause) {
        super(cause);
    }

    public NoSuchCommandException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}