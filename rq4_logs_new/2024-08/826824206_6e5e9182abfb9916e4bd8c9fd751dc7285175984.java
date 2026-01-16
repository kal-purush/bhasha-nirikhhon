package br.com.fiap.totem_express.presentation.errors;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestControllerAdvice
public class ErrorHandler {

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public BadRequestError handleValidationError(MethodArgumentNotValidException exception){
        return Stream
                .concat(
                    exception.getFieldErrors().stream().map(it -> new FieldError(it.getField(), it.getDefaultMessage())),
                    exception.getGlobalErrors().stream().map(it -> new FieldError(it.getCode(), it.getDefaultMessage()))
                )
                .collect(Collectors.collectingAndThen(
                    Collectors.toList(),
                    BadRequestError::new
                ));
    }

}