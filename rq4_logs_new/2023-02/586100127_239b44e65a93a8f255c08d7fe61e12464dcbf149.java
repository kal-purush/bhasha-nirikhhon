package com.example.cargo_transportation.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.List;

@ResponseStatus(HttpStatus.BAD_REQUEST)
public class ValidationException extends RuntimeException{

    private static final String DEFAULT_MESSAGE = "Ошибка валидации, проверьте ваши поля";
    private List<String> messages;

    public ValidationException(List<String> messages) {
        super(DEFAULT_MESSAGE);
        this.messages = messages;
    }
    public ValidationException(String message){
        super(message);
    }
}