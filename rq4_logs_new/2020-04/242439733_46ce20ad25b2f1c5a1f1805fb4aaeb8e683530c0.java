package umgc.city.team1.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
public class EmailException extends Exception {
    public EmailException() {
        super();
    }

    public EmailException(String message) {
        super(message);
    }

    public EmailException(Throwable e) {
        super(e);
    }

    public EmailException(String message, Throwable e) {
        super(message, e);
    }
}