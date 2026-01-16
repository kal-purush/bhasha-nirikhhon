package com.horapp.presentation.dto.response;

import org.springframework.http.HttpStatus;

import java.util.Map;

public class ValidationExceptionResponse extends ExceptionResponse{
    private Map<String, String> validationErrors;

    public ValidationExceptionResponse() {}

    public ValidationExceptionResponse(
            String backendMessage,
            HttpStatus status,
            String method,
            String endpoint,
            Map<String, String> validationErrors
    ) {
        super(backendMessage, status, method, endpoint);
        this.validationErrors = validationErrors;
    }

    public Map<String, String> getValidationErrors() {
        return validationErrors;
    }

    public void setValidationErrors(Map<String, String> validationErrors) {
        this.validationErrors = validationErrors;
    }
}