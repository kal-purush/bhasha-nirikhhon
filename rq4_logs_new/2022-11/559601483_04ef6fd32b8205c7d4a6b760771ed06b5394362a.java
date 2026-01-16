package com.soteria.controller_advice;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import com.soteria.exceptions.user.*;

@ControllerAdvice
public class UserExceptionControllerAdvice {
    @ExceptionHandler(EmptyPassword.class)
    public ResponseEntity<?> handleException(EmptyPassword e) {
        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(e.getMessage());
    }

    @ExceptionHandler(UserAlreadyExists.class)
    public ResponseEntity<?> handleException(UserAlreadyExists e) {
        return ResponseEntity
                .status(HttpStatus.CONFLICT)
                .body(e.getMessage());
    }

    @ExceptionHandler(UserNotFound.class)
    public ResponseEntity<?> handleException(UserNotFound e) {
        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(e.getMessage());
    }
}