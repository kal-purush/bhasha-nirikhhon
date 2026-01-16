package org.skypro.skyshop.model.controller;

import org.skypro.skyshop.model.exception.NoSuchProductException;
import org.skypro.skyshop.model.exception.ShopError;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ShopControllerAdvice {
    @ExceptionHandler(NoSuchProductException.class)
    public ResponseEntity<ShopError> handleNoSuchProductException(NoSuchProductException e) {
        ShopError error = new ShopError("NOT_FOUND", "Товар с указанным ID не найден");
        return new ResponseEntity<>(error, HttpStatus.NOT_FOUND);
    }
}