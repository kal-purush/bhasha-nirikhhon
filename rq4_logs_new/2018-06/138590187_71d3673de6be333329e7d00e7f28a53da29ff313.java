package com.example.demo.exception;

public class UsuariNotFoundException extends RuntimeException {

    public UsuariNotFoundException(String exception) {
        super(exception);
    }
}