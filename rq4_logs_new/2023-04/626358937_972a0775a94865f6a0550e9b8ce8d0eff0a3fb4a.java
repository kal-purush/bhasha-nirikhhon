package tripagramex.global.exception.advice;

import org.hibernate.annotations.Comment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import tripagramex.global.exception.BusinessLogicException;
import tripagramex.global.exception.ExceptionCode;
import tripagramex.global.exception.dto.ErrorResponse;

import java.net.BindException;

@Component
@RestControllerAdvice
public class ExceptionAdvice {

    @ExceptionHandler
    public ResponseEntity<ErrorResponse> basicExceptionHandler(Exception e) {

        ErrorResponse errorResponse =
                new ErrorResponse(e.getClass().getSimpleName(), e.getMessage(), ExceptionCode.GLOBAL_EXCEPTION.getCode());

        return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler
    public ResponseEntity<ErrorResponse> businessLoginExceptionHandler(BusinessLogicException e) {
        ExceptionCode exceptionCode = e.getExceptionCode();
        ErrorResponse errorResponse =
                new ErrorResponse(e.getClass().getSimpleName(), e.getMessage(), exceptionCode.getCode());

        return new ResponseEntity<>(errorResponse, HttpStatus.valueOf(exceptionCode.getStatus()));
    }

    @ExceptionHandler
    public ResponseEntity<ErrorResponse> bindExceptionHandler(BindException e) {

        ExceptionCode bindException = ExceptionCode.BIND_EXCEPTION;
        ErrorResponse errorResponse =
                new ErrorResponse(e.getClass().getSimpleName(), e.getMessage(), bindException.getCode());

        return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
    }
}