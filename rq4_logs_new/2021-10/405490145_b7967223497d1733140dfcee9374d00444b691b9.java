package kz.onggar.collector.configuration;

import kz.onggar.collector.exception.AlreadyExistException;
import kz.onggar.collector.exception.ResourceNotFoundException;
import kz.onggar.collector.exception.UnexpectedExecutionStateException;
import kz.onggar.collector.openapi.dto.Error;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.time.OffsetDateTime;

@ControllerAdvice(basePackages = "kz.onggar.collector.controller")
public class ExceptionConfiguration extends ResponseEntityExceptionHandler {
    @ResponseBody
    @ExceptionHandler(AlreadyExistException.class)
    public ResponseEntity<?> handleAlreadyExist(AlreadyExistException ex) {
        return new ResponseEntity<>(
                new Error()
                        .message(ex.getMessage())
                        .shortMessage(ex.getShortMessage())
                        .time(OffsetDateTime.now()),
                HttpStatus.CONFLICT
        );
    }

    @ResponseBody
    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<?> handleResourceNotFound(ResourceNotFoundException ex) {
        return new ResponseEntity<>(
                new Error()
                        .message(ex.getMessage())
                        .shortMessage(ex.getShortMessage())
                        .time(OffsetDateTime.now()),
                HttpStatus.NOT_FOUND
        );
    }

    @ResponseBody
    @ExceptionHandler(UnexpectedExecutionStateException.class)
    public ResponseEntity<?> handleUnexpectedExecutionState(UnexpectedExecutionStateException ex) {
        return new ResponseEntity<>(
                new Error()
                        .message(ex.getMessage())
                        .shortMessage(ex.getShortMessage())
                        .time(OffsetDateTime.now()),
                HttpStatus.INTERNAL_SERVER_ERROR
        );
    }

    @ResponseBody
    @ExceptionHandler(Throwable.class)
    public ResponseEntity<?> handleUnhandledErrors(Throwable ex) {
        return new ResponseEntity<>(
                new Error()
                        .message(ex.getMessage())
                        .time(OffsetDateTime.now()),
                HttpStatus.INTERNAL_SERVER_ERROR
        );
    }
}