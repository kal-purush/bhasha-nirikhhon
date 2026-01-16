package com.lotdiz.adminservice.exception.common;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;

@Getter
public abstract class DomainException extends RuntimeException {
    private final Map<String, String> validation = new HashMap<>();

    public DomainException(String message) {
        super(message);
    }

    public abstract int getStatusCode();

    public void addValidation(String fieldName, String errorMessage) {
        validation.put(fieldName, errorMessage);
    }
}