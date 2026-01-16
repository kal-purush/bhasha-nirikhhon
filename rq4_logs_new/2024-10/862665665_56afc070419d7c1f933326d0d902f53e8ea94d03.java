package com.prueba.customer.controller;

import com.prueba.customer.exception.ClienteException;
import com.prueba.customer.exception.ClienteNotFoundException;
import com.prueba.customer.exception.PersonaException;
import com.prueba.customer.exception.PersonaNotFoundException;
import com.prueba.customer.repository.entity.Cliente;
import com.prueba.customer.repository.entity.CustomError;
import com.prueba.customer.repository.entity.Persona;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import javax.imageio.ImageIO;

@ControllerAdvice
public class GlobalExceptionHandler {

    public static HttpHeaders headers = new HttpHeaders();

    @ExceptionHandler(PersonaNotFoundException.class)
    public ResponseEntity<Persona> handlePersonaNotFoundException(PersonaNotFoundException ex) {
        CustomError error = CustomError.builder()
                .codeErro("404")
                .messageError(ex.getMessage())
                .build();
        Persona p = new Persona();
        p.setError(error);
        headers.add("Content-Type", "application/json");
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(p);
    }

    @ExceptionHandler(ClienteNotFoundException.class)
    public ResponseEntity<Cliente> handlePersonaNotFoundException(ClienteNotFoundException ex) {
        CustomError error = CustomError.builder()
                .codeErro("404")
                .messageError(ex.getMessage())
                .build();
        Cliente c = new Cliente();
        c.setError(error);
        headers.add("Content-Type", "application/json");
        return ResponseEntity.status(HttpStatus.NOT_FOUND).headers(headers).body(c);
    }


    @ExceptionHandler(PersonaException.class)
    public ResponseEntity<Persona> handleGeneralException(PersonaException ex) {
        CustomError error = CustomError.builder()
                .codeErro("999")
                .messageError(ex.getMessage())
                .build();
        Persona p = new Persona();
        p.setError(error);
        headers.add("Content-Type", "application/json");
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).headers(headers).body(p);
    }

    @ExceptionHandler(ClienteException.class)
    public ResponseEntity<Cliente> handleGeneralException(ClienteException ex) {
        CustomError error = CustomError.builder()
                .codeErro("404")
                .messageError(ex.getMessage())
                .build();
        Cliente c = new Cliente();
        c.setError(error);
        headers.add("Content-Type", "application/json");
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).headers(headers).body(c);
    }
}