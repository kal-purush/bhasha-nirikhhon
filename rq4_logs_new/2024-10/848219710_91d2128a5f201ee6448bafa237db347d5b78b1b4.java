package com.example.spring.exception;

public class NotUniqueCartItem extends RuntimeException {
    public NotUniqueCartItem(String message, Throwable cause) {
        super(message, cause);
    }
}