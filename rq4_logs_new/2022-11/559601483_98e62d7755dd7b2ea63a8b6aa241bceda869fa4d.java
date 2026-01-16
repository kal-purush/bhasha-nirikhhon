package com.soteria.controller_advice;

import com.soteria.exceptions.credential.CredentialAlreadyExists;
import com.soteria.exceptions.credential.CredentialNotFound;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class CredentialExceptionControllerAdvice {
    @ExceptionHandler(CredentialAlreadyExists.class)
    public ResponseEntity<?> handleException(CredentialAlreadyExists e) {
        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(e.getMessage());
    }

    @ExceptionHandler(CredentialNotFound.class)
    public ResponseEntity<?> handleException(CredentialNotFound e) {
        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(e.getMessage());
    }
}