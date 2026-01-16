package bit.exception;

import jakarta.persistence.EntityNotFoundException;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(value = EntityNotFoundException.class)
    public ResponseEntity<String> entityNotFoundException(EntityNotFoundException e, HttpServletRequest request) {
        log.error("Error Code : {}, Url : {}, message : {}", HttpStatus.NOT_FOUND, request.getRequestURI(), e.getMessage(), e);
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
    }

    @ExceptionHandler(value = HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<String> badRequestMethodException(HttpRequestMethodNotSupportedException e, HttpServletRequest request) {
        log.error("Error Code : {}, Url : {}, message : {}", HttpStatus.BAD_REQUEST, request.getRequestURI(), e.getMessage(), e);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
    }

    @ExceptionHandler(value = MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, String>> methodArgumentNotValidException(MethodArgumentNotValidException e, HttpServletRequest request) {
        Map<String, String> errorMessages = new HashMap<>();
        e.getBindingResult().getFieldErrors().forEach(error ->
                errorMessages.put(error.getField(), error.getDefaultMessage()));

        log.error("Error Code : {}, Url : {}, message : {}", HttpStatus.BAD_REQUEST, request.getRequestURI(), errorMessages, e);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorMessages);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<String> internalServerException(Exception e, HttpServletRequest request) {
        log.error("Error Code : {}, Url : {}, message : {}", HttpStatus.INTERNAL_SERVER_ERROR, request.getRequestURI(), e.getMessage(), e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Internal Server Error");
    }
}