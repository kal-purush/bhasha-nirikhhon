package gigjob.common.exception.handler;

import gigjob.common.exception.model.AudienceMismatchException;
import gigjob.common.exception.model.ClientIdMismatchException;
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
 * Handler for Firebase exceptions
 */
@CrossOrigin
@RestControllerAdvice
@Log4j2
public class FirebaseExceptionHandler extends ResponseEntityExceptionHandler {
    /**
     * Handle client id of id Token not found
     *
     * @param exception {@link  ClientIdMismatchException}
     * @return {@link ErrorResponse}
     * @author Vien Binh
     */
    @ExceptionHandler(ClientIdMismatchException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleClientIdMismatchException(ClientIdMismatchException exception) {
        log.error(exception.getMessage());
        return new ErrorResponse(new Date(), HttpStatus.BAD_REQUEST.toString(), exception.getMessage());
    }

    /**
     * Handle client id of id Token not found
     *
     * @param exception {@link  AudienceMismatchException}
     * @return {@link ErrorResponse}
     * @author Vien Binh
     */
    @ExceptionHandler(AudienceMismatchException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleAudienceMismatchException(AudienceMismatchException exception) {
        log.error(exception.getMessage());
        return new ErrorResponse(new Date(), HttpStatus.BAD_REQUEST.toString(), exception.getMessage());
    }


}