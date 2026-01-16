package com.petmeet.api.api.advice;

import com.petmeet.api.api.controller.dto.response.BaseApiResponse;
import com.petmeet.api.core.exception.BaseCustomException;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.TypeMismatchException;
import org.springframework.http.HttpStatus;
import org.springframework.validation.BindException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingPathVariableException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@Slf4j
@RestControllerAdvice
public class ExceptionControllerAdvice {

    // 커스텀 예외 처리
    @ExceptionHandler(BaseCustomException.class)
    public BaseApiResponse<Void> handleCustomExceptions(BaseCustomException ex) {
        log.error("Custom exception occurred: {}", ex.getMessage());

        return BaseApiResponse.error(ex.getErrorCode());
    }

    // 유효성 검증 관련 예외 처리
    @ExceptionHandler({
            MethodArgumentNotValidException.class,
            ConstraintViolationException.class,
            BindException.class,
            TypeMismatchException.class,
            MissingServletRequestParameterException.class,
            MethodArgumentTypeMismatchException.class,
            MissingPathVariableException.class})
    public BaseApiResponse<Void> handleValidationExceptions(Exception ex) {
        log.error("Validation exception occurred: {}", ex.getMessage());

        return BaseApiResponse.error(HttpStatus.BAD_REQUEST, "Validation exception occurred");
    }

    // unchecked 예외 처리
    @ExceptionHandler(RuntimeException.class)
    public BaseApiResponse<Void> handleRuntimeExceptions(RuntimeException ex) {
        log.error("Runtime exception occurred: {}", ex.getMessage());

        return BaseApiResponse.error(HttpStatus.INTERNAL_SERVER_ERROR, "Runtime exception occurred");
    }

    // checked 예외 처리
    @ExceptionHandler(Exception.class)
    public BaseApiResponse<Void> handleExceptions(Exception ex) {
        log.error("General exception occurred: {}", ex.getMessage());

        return BaseApiResponse.error(HttpStatus.INTERNAL_SERVER_ERROR, "General exception occurred");
    }
}