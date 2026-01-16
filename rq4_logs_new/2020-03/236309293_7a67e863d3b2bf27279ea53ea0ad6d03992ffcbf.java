package com.skybreak.application.exception;

import java.util.Collections;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(UserNotFoundException.class)
    protected ResponseEntity<?> handleUserNotFound(final CodeWarsException exception, WebRequest request) {
        List<String> errors = Collections.singletonList(exception.getMessage());
        return handleExceptionInternal(exception, errors, null, HttpStatus.NOT_FOUND, request);
    }

    @ExceptionHandler(UsernameNotValidException.class)
    protected ResponseEntity<?> handleUsernameNotValid(final CodeWarsException exception, WebRequest request) {
        List<String> errors = Collections.singletonList(exception.getMessage());
        return handleExceptionInternal(exception, errors, null, HttpStatus.BAD_REQUEST, request);
    }

    @ExceptionHandler(IncorrectPasswordException.class)
    protected ResponseEntity<?> handleIncorrectPassword(final CodeWarsException exception, WebRequest request) {
        List<String> errors = Collections.singletonList(exception.getMessage());
        return handleExceptionInternal(exception, errors, null, HttpStatus.BAD_REQUEST, request);
    }

}