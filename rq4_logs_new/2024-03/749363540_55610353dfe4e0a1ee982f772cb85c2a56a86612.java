package ru.practicum.shareit.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;


/**
 * Global error handler for the Filmorate application.
 */
@RestControllerAdvice
public class ErrorHandler {

    /**
     * Handles ValidationException and returns a BAD_REQUEST response.
     *
     * @param e The ValidationException.
     * @return ErrorResponse with the error message.
     */
    @ExceptionHandler
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleValidationException(final ValidationException e) {
        return new ErrorResponse(
                e.getMessage()
        );
    }

    /**
     * Handles IncorrectParameterException and returns a BAD_REQUEST response.
     *
     * @param e The IncorrectParameterException.
     * @return ErrorResponse with the error message.
     */
    @ExceptionHandler
    @ResponseStatus(HttpStatus.CONFLICT)
    public ErrorResponse handleIncorrectParameterException(final IncorrectParameterException e) {
        return new ErrorResponse(
                String.format("Ошибка с полем \"%s\".", e.getParameter())
        );
    }

    /**
     * Handles NotFoundException and returns a NOT_FOUND response.
     *
     * @param e The NotFoundException.
     * @return ErrorResponse with the error message.
     */
    @ExceptionHandler
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponse handleNotFoundException(final NotFoundException e) {
        return new ErrorResponse(
                e.getMessage()
        );
    }

    /**
     * Handles Throwable (general exception) and returns an INTERNAL_SERVER_ERROR response.
     *
     * @param e The Throwable.
     * @return ErrorResponse with a generic error message.
     */
    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorResponse handleThrowable(final Throwable e) {
        return new ErrorResponse(
                "Произошла непредвиденная ошибка."
        );
    }
}