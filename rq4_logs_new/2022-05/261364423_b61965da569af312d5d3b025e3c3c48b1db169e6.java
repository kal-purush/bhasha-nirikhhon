package elixter.blog.exception;

import org.jetbrains.annotations.NotNull;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalControllerAdvice {

    @ExceptionHandler(RestException.class)
    public ResponseEntity<Map<String, Object>> handler(RestException e) {
        Map<String, Object> response = e.getRestExceptionResponseMap();

        return new ResponseEntity<>(response, e.getStatus());
    }


    @ExceptionHandler(InvalidBodyFieldException.class)
    public ResponseEntity<Map<String, Object>> invalidBodyFieldExceptionHandler(InvalidBodyFieldException e) {
        Map<String, Object> response = e.getFieldErrorsMap();
        response.put("fieldInfo", e.getFieldErrorsMap());

        return new ResponseEntity<>(response, e.getStatus());
    }
}