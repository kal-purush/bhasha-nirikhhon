//package com.angkorsuntrix.demosynctrix.exception;
//
//import org.springframework.http.HttpStatus;
//import org.springframework.http.ResponseEntity;
//import org.springframework.web.bind.annotation.ControllerAdvice;
//import org.springframework.web.bind.annotation.ExceptionHandler;
//
//
//@ControllerAdvice
//public class ApiExceptionHandler {
//
//    @ExceptionHandler(value = ResponseException.class)
//    public ResponseEntity<Object> handleApiResponseException(ResponseException e) {
//        return new ResponseEntity<>(e, HttpStatus.CONFLICT);
//    }
//}