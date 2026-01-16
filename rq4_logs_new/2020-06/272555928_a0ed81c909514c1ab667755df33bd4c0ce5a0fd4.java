package br.com.orion.bank.exceptions.handler;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;

import org.hibernate.validator.internal.engine.path.PathImpl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import br.com.orion.bank.exceptions.PaymentException;
import br.com.orion.bank.model.dto.MessageReturnDto;

/**
 * ApiExceptionHandler
 */
@RestControllerAdvice
public class ApiExceptionHandler  {

    @ExceptionHandler(PaymentException.class)
    public ResponseEntity<?> handlerPaymentException(PaymentException ex) {
        MessageReturnDto messageReturn = MessageReturnDto.builder().message(ex.getMessage()).build();

       // return new ResponseEntity<>(messageReturn, HttpStatus.NOT_ACCEPTABLE);
       return ResponseEntity.status(HttpStatus.NOT_ACCEPTABLE).body(messageReturn);
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<?> handlePropertyReferenceException(ConstraintViolationException exception) {
        Set<ConstraintViolation<?>> fieldErros = exception.getConstraintViolations();

        Map<String, String> errors = new HashMap<>();
        for (ConstraintViolation<?> fe : fieldErros) {      
            String fieldName = ((PathImpl)fe.getPropertyPath()).getLeafNode().getName();  
            errors.put(fieldName, fe.getMessage());
        }

        MessageReturnDto messageReturn = MessageReturnDto.builder()
        .message("Invalid arguments")
        .errors(errors)
         .build();


        return new ResponseEntity<>(messageReturn, HttpStatus.BAD_REQUEST);
    }

}