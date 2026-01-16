package org.embulk.util.rubytime;

public class RubyTimeParseException extends RuntimeException {
    public RubyTimeParseException(final String message) {
        super(message);
    }

    public RubyTimeParseException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public RubyTimeParseException(final Throwable cause) {
        super(cause);
    }
}