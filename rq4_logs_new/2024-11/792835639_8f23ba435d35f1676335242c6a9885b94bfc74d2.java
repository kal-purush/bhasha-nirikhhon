package com.jinius.ecommerce.common;

import java.util.ArrayList;
import java.util.List;

public class ValidationErrorResponse {

    private List<FieldError> errors = new ArrayList<>();

    public void addError(String field, String message) {
        this.errors.add(new FieldError(field, message));
    }

    public static class FieldError {
        private String field;
        private String message;

        public FieldError(String field, String message) {
            this.field = field;
            this.message = message;
        }

        public String getField() {
            return field;
        }

        public String getMessage() {
            return message;
        }
    }

    public List<FieldError> getErrors() {
        return errors;
    }
}