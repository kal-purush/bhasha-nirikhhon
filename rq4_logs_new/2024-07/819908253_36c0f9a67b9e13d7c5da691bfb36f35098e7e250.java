package com.wojciechbarwinski.demo.legendary_warehouse.controllers;

import com.wojciechbarwinski.demo.legendary_warehouse.dtos.MissingProductDTO;
import com.wojciechbarwinski.demo.legendary_warehouse.exceptions.ErrorResponse;
import com.wojciechbarwinski.demo.legendary_warehouse.exceptions.MissingProductException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@ControllerAdvice
public class ApplicationExceptionHandlerController {


    @ResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY) // 422
    @ExceptionHandler(MissingProductException.class)
    public ErrorResponse<List<MissingProductDTO>> missingProductException(MissingProductException exception){

        return new ErrorResponse<>(exception.getMessage(), exception.getMissingProducts());
    }
}