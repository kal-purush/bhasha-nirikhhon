package groupone.userservice.aop;

import groupone.userservice.dto.response.ErrorResponse;
import org.apache.http.auth.InvalidCredentialsException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class AOPExceptionHandler {
    @ExceptionHandler(value = {Exception.class})
    public ResponseEntity<ErrorResponse> handleException(Exception e) {
        return new ResponseEntity<>(ErrorResponse.builder().message("Exception: " + e.getMessage()).build(), HttpStatus.OK);
    }

    @ExceptionHandler(value = {DataIntegrityViolationException.class})
    public ResponseEntity<ErrorResponse> handleDataIntegrityViolationException(Exception e) {
        return new ResponseEntity<>(ErrorResponse.builder().message("DataIntegrityViolationException: " + e.getMessage()).build(), HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(value = {BadCredentialsException.class})
    public ResponseEntity<ErrorResponse> handleBadCredentialsException(Exception e) {
        return new ResponseEntity<>(ErrorResponse.builder().message("BadCredentialsException: " + e.getMessage()).build(), HttpStatus.UNAUTHORIZED);
    }

    @ExceptionHandler(value = {InvalidCredentialsException.class})
    public ResponseEntity<ErrorResponse> handleInvalidCredentialsException(Exception e) {
        return new ResponseEntity<>(ErrorResponse.builder().message("InvalidCredentialsException: " + e.getMessage()).build(), HttpStatus.BAD_REQUEST);
    }
}