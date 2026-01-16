package ru.practicum.shareit.exception;

import org.hibernate.ObjectNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ErrorHandlerGeneralTest {

    private ErrorHandlerGeneral errorHandlerGeneral;

    @BeforeEach
    void setUp() {
        this.errorHandlerGeneral = new ErrorHandlerGeneral();
    }

    @Test
    void handleValidationException() {
        ValidationException validationException = new ValidationException("validation-exception");
        ErrorResponse expected = new ErrorResponse("Validation error 409", "validation-exception");
        ErrorResponse result = errorHandlerGeneral.handlerValidationException(validationException);
        assertEquals(expected, result);
    }

    @Test
    void handleNotFoundException() {
        ObjectNotFoundException notFoundException = new ObjectNotFoundException(1L, "object-not-found");
        ErrorResponse expected = new ErrorResponse("Object not found 404", "No row with the given identifier exists: [object-not-found#1]");
        ErrorResponse result = errorHandlerGeneral.handlerNotFoundException(notFoundException);
        assertEquals(expected, result);
    }

    @Test
    void handleDataExistException() {
        DataExistException dataExistException = new DataExistException("conflict-exception");
        ErrorResponse expected = new ErrorResponse("409 Conflict", "conflict-exception");
        ErrorResponse result = errorHandlerGeneral.handlerDataExistException(dataExistException);
        assertEquals(expected, result);
    }

    @Test
    void handlerDuplicateException() {
        DuplicateEmailException dataExistException = new DuplicateEmailException("duplicate-exception");
        ErrorResponse expected = new ErrorResponse("DuplicateEmailException error 409", "duplicate-exception");
        ErrorResponse result = errorHandlerGeneral.handlerDuplicateException(dataExistException);
        assertEquals(expected, result);
    }

}