package ru.practicum.shareit.exeption;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.ServletWebRequest;
import org.springframework.web.context.request.WebRequest;
import ru.practicum.shareit.exceptions.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ErrorHandlerTest {

    @Test
    void handleObjectNotFoundException() {
        ErrorHandler errorHandler = new ErrorHandler();
        DataNotFoundException notFoundException = new DataNotFoundException("Not found");
        WebRequest request = new ServletWebRequest(new MockHttpServletRequest());
        ErrorResponse response = errorHandler.handleObjectNotFoundException(notFoundException);
        assertEquals("Not found", response.getError());
    }

    @Test
    void handleValidationException() {
        ErrorHandler errorHandler = new ErrorHandler();
        UserAlreadyExistsException validationException = new UserAlreadyExistsException("Validation error");
        WebRequest request = new ServletWebRequest(new MockHttpServletRequest());
        ErrorResponse response = errorHandler.handleValidationException(validationException);
        assertEquals("Validation error", response.getError());
    }

    @Test
    void handleBadRequestException() {
        ErrorHandler errorHandler = new ErrorHandler();
        BadRequestException badRequestException = new BadRequestException("Runtime error");
        WebRequest request = new ServletWebRequest(new MockHttpServletRequest());

        ErrorResponse response = errorHandler.handleBadRequestException(badRequestException);

        assertEquals("Runtime error", response.getError());
    }
}