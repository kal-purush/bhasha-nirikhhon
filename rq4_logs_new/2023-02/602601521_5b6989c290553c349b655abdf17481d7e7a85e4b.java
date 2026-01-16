package com.rafaeldeluca.Ecommerce.Java.crontrollers.handlers;

import com.rafaeldeluca.Ecommerce.Java.dto.CustomError;
import com.rafaeldeluca.Ecommerce.Java.services.exceptions.ResourceNotFoundException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.time.Instant;

@ControllerAdvice
public class ControllerExceptionHandler {
    // handler vão ser as classes de manipulação
    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<CustomError> recursoNaoEncontrado(ResourceNotFoundException erro, HttpServletRequest requisicao) {
        HttpStatus statusDoErro = HttpStatus.NOT_FOUND;
        CustomError errorCustomizado = new CustomError(Instant.now(), statusDoErro.value(), erro.getMessage(),requisicao.getRequestURI());
        return ResponseEntity.status(statusDoErro).body(errorCustomizado);

    }
}