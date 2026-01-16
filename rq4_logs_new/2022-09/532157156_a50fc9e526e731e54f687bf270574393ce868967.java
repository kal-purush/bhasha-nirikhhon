package com.bridgelabz.bookstore.advice;

import com.bridgelabz.bookstore.exceptions.MultipleUsers;
import com.bridgelabz.bookstore.exceptions.UserNotFound;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class RegisterUserAdvice {
    @ExceptionHandler(value = {UserNotFound.class,NullPointerException.class})
    public ResponseEntity<APIResponse> userNotFound(Exception userNotFound) {
        APIResponse apiResponse = new APIResponse();
        apiResponse.setStatus(HttpStatus.BAD_REQUEST.value());
        apiResponse.setError(userNotFound.getMessage());
        return ResponseEntity.ok().body(apiResponse);
    }
    @ExceptionHandler(value = MultipleUsers.class)
    public ResponseEntity<APIResponse> userNotFound(MultipleUsers multipleUsers) {
        APIResponse apiResponse = new APIResponse();
        apiResponse.setStatus(HttpStatus.BAD_REQUEST.value());
        apiResponse.setError(multipleUsers.getMessage());
        return ResponseEntity.ok().body(apiResponse);
    }
}