package com.example.librarymanagement.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.CONFLICT)
public class DuplicateKeyException extends RuntimeException {
    private String resourseName;
    private String fieldName;

    public DuplicateKeyException(String resourceName, String fieldName) {
        super(String.format("%s already exists with %s", resourceName, fieldName));
        this.resourseName = resourceName;
        this.fieldName = fieldName;
    }
}