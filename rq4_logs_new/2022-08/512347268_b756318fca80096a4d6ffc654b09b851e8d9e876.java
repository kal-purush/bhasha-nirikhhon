package com.cakestation.backend.common.handler.exception;

import com.cakestation.backend.common.handler.exception.error.ErrorMessage;
import com.cakestation.backend.common.handler.exception.error.ErrorMessageFactory;
import com.cakestation.backend.store.exception.InvalidStoreIdException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ErrorHandler {

    @ExceptionHandler(InvalidStoreIdException.class)
    public ResponseEntity<ErrorMessage> invalidStoreIdExceptionHandler(InvalidStoreIdException e){
        return ResponseEntity.status(e.getStatus())
                .body(ErrorMessageFactory.from(e.getStatus(), e.getMessage()));
    }
}