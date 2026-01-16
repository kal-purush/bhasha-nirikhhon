package ru.clevertec.carshowroom.controller;

import jakarta.validation.ValidationException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ru.clevertec.carshowroom.exception.CarNotFoundException;
import ru.clevertec.carshowroom.exception.CarShowroomNotFoundException;
import ru.clevertec.carshowroom.exception.CategoryNotFoundException;
import ru.clevertec.carshowroom.exception.ClientNotFoundException;
import ru.clevertec.carshowroom.exception.ReviewNotFoundException;

@RestControllerAdvice
public class ControllerAdvice {

    @ExceptionHandler(value = Exception.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ExceptionObject response400(@RequestBody Exception exception) {
        return aggregate(exception.getMessage(), HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(value = {
            ValidationException.class,
            MethodArgumentNotValidException.class
    })
    @ResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY)
    public ExceptionObject response422(@RequestBody Exception exception) {
        return aggregate(exception.getLocalizedMessage(), HttpStatus.UNPROCESSABLE_ENTITY);
    }

    @ExceptionHandler(value = {
            CarNotFoundException.class,
            CarShowroomNotFoundException.class,
            CategoryNotFoundException.class,
            ClientNotFoundException.class,
            ReviewNotFoundException.class
    })
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ExceptionObject response404(@RequestBody Exception exception) {
        return aggregate(exception.getMessage(), HttpStatus.NOT_FOUND);
    }

    private ExceptionObject aggregate(String message, HttpStatus httpStatus) {
        return new ExceptionObject(httpStatus.value(), httpStatus.name(), message);
    }

    public record ExceptionObject(int code, String status, String message) {}
}