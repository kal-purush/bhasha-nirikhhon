package com.example.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Created by kostya.nikitin on 9/2/2016.
 */
@RestControllerAdvice
public class CitiesFromDBControllerAdvice {
    final Logger slf4jLog = LoggerFactory.getLogger(CitiesFromDBControllerAdvice.class);

    @ExceptionHandler(Exception.class)
    ResponseEntity<?> allErrorExceptionHandler(Throwable ex) {
        slf4jLog.error(" Error handling: " + ex.getClass() + " with message " + ex.getMessage());
        return new ResponseEntity<>(ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }

}