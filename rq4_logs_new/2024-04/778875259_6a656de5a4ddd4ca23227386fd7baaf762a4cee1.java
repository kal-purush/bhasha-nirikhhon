package me.dio.sdw24.adptares.in.exception;

import me.dio.sdw24.domain.exception.ChampionNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class GlobalExceptionHandler {

    private Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ChampionNotFoundException.class)
    public ResponseEntity<ApiError> handlerDomainException(ChampionNotFoundException domainError) {
        logger.error(domainError.getMessage(), domainError);
        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
//                .unprocessableEntity() // response status is 422
                .body( new ApiError(domainError.getMessage()) );
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handlerDomainException(Exception unexpectedError) {
        String mensagem = "Ops! Um error ocorreu.";
        logger.error(mensagem, unexpectedError);
        return ResponseEntity
                .internalServerError()
                .body( new ApiError(mensagem) );
    }

    public record ApiError(String message) { }
}