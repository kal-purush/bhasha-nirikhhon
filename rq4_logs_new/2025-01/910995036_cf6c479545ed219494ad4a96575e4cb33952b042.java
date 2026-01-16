package com.example.lifeonhana.global.exception;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.example.lifeonhana.ApiResponse;

@RestControllerAdvice
public class GlobalExceptionHandler {
	@ExceptionHandler(BaseException.class)
	public ResponseEntity<?> handleApiException(BaseException exception) {
		ApiResponse response = ApiResponse.builder()
			.code(exception.getStatusCode())
			.status(exception.getHttpStatus())
			.message(exception.getCustomMessage())
			.data(null)
			.build();
		return ResponseEntity.status(exception.getHttpStatus()).body(response);
	}
}