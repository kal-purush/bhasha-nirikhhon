package exception.game;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus (value= HttpStatus.NOT_FOUND, reason="No such Game")
public class GameNotFoundException extends GameException {
  public GameNotFoundException() {
  }

  public GameNotFoundException(String message) {
    super(message);
  }

  public GameNotFoundException(Throwable throwable) {
    super(throwable);
  }

  public GameNotFoundException(String message, Throwable throwable) {
    super(message, throwable);
  }
}