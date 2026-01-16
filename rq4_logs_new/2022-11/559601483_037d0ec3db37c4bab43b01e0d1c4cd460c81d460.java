package com.soteria.controller_advice;

import com.soteria.exceptions.role.RoleAlreadyExists;
import com.soteria.exceptions.role.RoleNotFound;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class RoleExceptionControllerAdvice {
    @ExceptionHandler(RoleNotFound.class)
    public ResponseEntity<?> handleException(RoleNotFound e) {
        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(e.getMessage());
    }

    @ExceptionHandler(RoleAlreadyExists.class)
    public ResponseEntity<?> handleException(RoleAlreadyExists e) {
        return ResponseEntity
                .status(HttpStatus.CONFLICT)
                .body(e.getMessage());
    }
}