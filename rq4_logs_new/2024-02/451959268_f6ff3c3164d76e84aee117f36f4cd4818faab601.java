package pl.todolist.exception;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import javax.persistence.EntityNotFoundException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class ApiExceptionHandlerTest {

    private final ApiExceptionHandler exceptionHandler = new ApiExceptionHandler();

    @Test
    @DisplayName("Should handle CustomIdException")
    void handleCustomIdException() {
        CustomIdException exception = new CustomIdException();
        ApiException apiException = new ApiException(exception.getMessage(), HttpStatus.UNPROCESSABLE_ENTITY);

        ResponseEntity<Object> result = exceptionHandler.handleCustomIdException(exception);

        assertThat(result.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(result.getBody()).isEqualTo(apiException);
    }

    @Test
    @DisplayName("Should handle EntityNotFoundException")
    void handleEntityNotFoundException() {
        EntityNotFoundException exception = new EntityNotFoundException();
        ApiException apiException = new ApiException(exception.getMessage(), HttpStatus.NOT_FOUND);

        ResponseEntity<Object> result = exceptionHandler.handleEntityNotFoundException(exception);

        assertThat(result.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(result.getBody()).isEqualTo(apiException);
    }
}