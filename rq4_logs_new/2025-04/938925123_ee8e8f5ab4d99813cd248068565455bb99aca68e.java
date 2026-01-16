package com.example.taskmanager.config.exception.classes.auth;

public class InactivatedUserException extends RuntimeException {
    public InactivatedUserException(String message) {
        super(message);
    }
}