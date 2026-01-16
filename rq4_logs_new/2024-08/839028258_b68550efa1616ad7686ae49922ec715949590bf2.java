package net.cakemc.networking.codec.exception;

public class DecodeException extends Throwable {

    public DecodeException(String message) {
        super(message);
    }

    public DecodeException() {
    }

    public DecodeException(String message, Throwable cause) {
        super(message, cause);
    }

    public DecodeException(Throwable cause) {
        super(cause);
    }

    public DecodeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}