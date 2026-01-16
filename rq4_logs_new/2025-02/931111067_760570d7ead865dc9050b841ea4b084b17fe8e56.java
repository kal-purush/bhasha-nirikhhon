package ru.practicum.main_service.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

@RestControllerAdvice
@Slf4j
public class ErrorHandler {
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @ExceptionHandler
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, String> handleNotFound(final NotFoundException e) {
        log.error("NotFoundException with message {} was thrown", e.getMessage());
        Map map = new LinkedHashMap<String, String>();
        map.put("status", HttpStatus.NOT_FOUND.name());
        map.put("reason", "The required object was not found.");
        map.put("message", e.getMessage());
        map.put("timestamp", LocalDateTime.now().format(formatter));
        return map;
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleMethodArgumentNotValid(final MethodArgumentNotValidException e) {
        log.error("MethodArgumentNotValidException with message {} was thrown", e.getMessage());
        Map map = new LinkedHashMap<String, String>();
        map.put("status", HttpStatus.BAD_REQUEST.name());
        map.put("reason", "Incorrectly made request.");
        map.put("message", e.getMessage());
        map.put("timestamp", LocalDateTime.now().format(formatter));
        return map;
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.CONFLICT)
    public Map<String, String> handleDataIntegrityViolation(final DataIntegrityViolationException e) {
        log.error("DataIntegrityViolationException with message {} was thrown", e.getMessage());
        Map map = new LinkedHashMap<String, String>();
        map.put("status", HttpStatus.CONFLICT.name());
        map.put("reason", "Integrity constraint has been violated.");
        map.put("message", e.getMessage());
        map.put("timestamp", LocalDateTime.now().format(formatter));
        return map;
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, String> handleException(final Exception e) {
        log.error("Exception with message {} was thrown", e.getMessage());
        Map map = new LinkedHashMap<String, String>();
        map.put("status", HttpStatus.INTERNAL_SERVER_ERROR.name());
        map.put("reason", "Something get wrong.");
        map.put("message", e.getMessage());
        map.put("timestamp", LocalDateTime.now().format(formatter));
        return map;
    }
}