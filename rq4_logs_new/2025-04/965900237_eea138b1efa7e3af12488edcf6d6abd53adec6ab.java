package skillfactory.DreamTeam.globus.it.controllers.advice;

import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;
import skillfactory.DreamTeam.globus.it.constants.error.SystemCode;
import skillfactory.DreamTeam.globus.it.exceptions.UserAlreadyExistsException;

import java.util.Date;

@Slf4j
@RestControllerAdvice
public class AuthControllerAdvice {

    @ExceptionHandler(value = UserAlreadyExistsException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorMessage handleUserAlreadyExistsException(UserAlreadyExistsException ex, HttpServletRequest request) {
        return ErrorMessage.builder()
                .statusCode(SystemCode.USER_ALREADY_EXISTS.getHttpCode())
                .timestamp(new Date())
                .message(ex.getMessage())
                .systemCode(SystemCode.USER_ALREADY_EXISTS.getSystemCode())
                .build();
    }

    @ExceptionHandler(value = BadCredentialsException.class)
    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    public ErrorMessage handleUserAlreadyExistsException(BadCredentialsException ex, HttpServletRequest request) {
        return ErrorMessage.builder()
                .statusCode(HttpStatus.BAD_REQUEST.value())
                .timestamp(new Date())
                .message(ex.getMessage())
                .systemCode(SystemCode.BAD_CREDENTIALS.getSystemCode())
                .build();
    }

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity handleResponseStatusException(ResponseStatusException e, HttpServletRequest request) {
        return ResponseEntity.status(e.getStatusCode()).build();
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorMessage handlePortalException(Throwable e, HttpServletRequest request) {
        log.error("Error: ", e);
        return ErrorMessage.builder()
                .statusCode(500)
                .timestamp(new Date())
                .message(e.getMessage())
                .systemCode(500)
                .path(request.getServletPath())
                .build();
    }
}