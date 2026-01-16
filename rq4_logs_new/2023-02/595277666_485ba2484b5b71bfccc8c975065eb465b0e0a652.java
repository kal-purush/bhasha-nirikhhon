package gigjob.common.exception.handler;

import gigjob.common.exception.model.JwtTokenException;
import gigjob.common.exception.model.UserNotFoundException;
import gigjob.dto.ErrorResponse;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.util.Date;

/**
 * Handler for each exception
 *
 * @author Vien Binh
 */
@CrossOrigin
@RestControllerAdvice
@Log4j2
public class CustomExceptionHandler extends ResponseEntityExceptionHandler {
    /**
     * Handle JWT authentication cause by unauthorized user
     *
     * @param exception {@link  JwtTokenException}
     * @return {@link ErrorResponse}
     * @author Vien Binh
     */
    @ExceptionHandler(JwtTokenException.class)
    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    public ErrorResponse handleJwtTokenException(JwtTokenException exception) {
        log.error(exception.getMessage());
        return new ErrorResponse(new Date(), HttpStatus.UNAUTHORIZED.toString(), exception.getMessage());
    }

    /**
     * Handle user not found in database
     *
     * @param exception {@link  UserNotFoundException}
     * @return {@link ErrorResponse}
     * @author Vien Binh
     */
    @ExceptionHandler(UserNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponse handleUserNotFoundException(UserNotFoundException exception) {
        log.error(exception.getMessage());
        return new ErrorResponse(new Date(), HttpStatus.NOT_FOUND.toString(), exception.getMessage());
    }
}