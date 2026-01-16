package exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus (value= HttpStatus.FORBIDDEN, reason="Invalid password")
public class InvalidUserCookieException extends UserException {
  public InvalidUserCookieException() {
  }

  public InvalidUserCookieException(String message) {
    super(message);
  }

  public InvalidUserCookieException(Throwable throwable) {
    super(throwable);
  }

  public InvalidUserCookieException(String message, Throwable throwable) {
    super(message, throwable);
  }
}