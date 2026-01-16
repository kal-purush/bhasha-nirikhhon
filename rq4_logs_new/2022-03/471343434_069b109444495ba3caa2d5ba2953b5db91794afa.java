package com.example.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;

@ControllerAdvice
public class ControllerExceptionHandler {

    @ExceptionHandler(value = {BadDataException.class})
    public ResponseEntity<Object> badDataException(BadDataException ex, WebRequest request) {
        return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }
}