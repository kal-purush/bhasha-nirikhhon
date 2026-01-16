package com.restfulmusicvideo.restfulmusicvideo.advice;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import com.restfulmusicvideo.restfulmusicvideo.common.Response;

@ControllerAdvice
public class GlobalExceptionHandler {
	@Autowired
	Response response;

	@ExceptionHandler
	public ResponseEntity<Response> handleException(Exception e) {
		// response.setError(e.getMessage());
		response.setError("Oops..Something went wrong!");
		response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());
		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR.value()).body(response);
	}

}