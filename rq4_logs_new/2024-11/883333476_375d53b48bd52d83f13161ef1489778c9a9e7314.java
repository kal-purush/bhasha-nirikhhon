package org.rentifytools.exception;

import jakarta.security.auth.message.AuthException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(AuthException.class)
    public ResponseEntity<AppError> handleAuthException(AuthException ex) {
        AppError appError = new AppError(
                ex.getMessage(),
                HttpStatus.UNAUTHORIZED
        );
        return new ResponseEntity<>(appError, appError.getStatus());
    }

    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<AppError> handleNotFoundException(NotFoundException ex) {
        AppError appError = new AppError(
                ex.getMessage(),
                HttpStatus.NOT_FOUND
        );
        return new ResponseEntity<>(appError, appError.getStatus());
    }

//    @ExceptionHandler(Exception.class)
//    public ResponseEntity<AppError> handleGenericException(Exception ex) {
//        AppError appError = new AppError(
//                "An unexpected error occurred: " + ex.getMessage(),
//                HttpStatus.INTERNAL_SERVER_ERROR
//        );
//        return new ResponseEntity<>(appError, appError.getStatus());
//    }
}
