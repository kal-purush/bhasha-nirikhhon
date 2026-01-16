package pl.todolist.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(DuplicatedValueOfUniqueFieldException.class)
    public ResponseEntity<Object> handleDuplicatedEntityException(DuplicatedValueOfUniqueFieldException e) {
        ApiException apiException = new ApiException(e.getMessage(), HttpStatus.UNPROCESSABLE_ENTITY);

        return new ResponseEntity<>(apiException, apiException.getStatus());
    }
}