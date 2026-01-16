package ru.itis.javawarrior.controller;


import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import static org.springframework.http.HttpStatus.*;


@ControllerAdvice
public class GlobalExceptionHandlerController extends ResponseEntityExceptionHandler {


    @ExceptionHandler(Exception.class)
    public ResponseEntity<String> exception(Exception e) {
        Throwable rootCause = e.getCause();
        return new ResponseEntity<>(rootCause.getMessage(),INTERNAL_SERVER_ERROR);
    }
}