package com.triviamachine.triviamachine.util;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.text.ParseException;

@ResponseStatus(code = HttpStatus.BAD_REQUEST, reason = "Content Not Found")
public class BadDateException extends ParseException {
    public BadDateException(String s, int i) {
        super(s, i);
    }
}