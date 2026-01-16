package com.sparta.communityback.exception;

import com.sparta.communityback.dto.ResultResponseDto;
import com.sparta.communityback.dto.StatusResponseDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AuthorizationServiceException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j(topic = "Global exception")
@RestControllerAdvice
public class GlobalExceptionHandler {

    // IllegalArgumentException 처리
    @ExceptionHandler({IllegalArgumentException.class})
    public ResponseEntity<StatusResponseDto> handleException(IllegalArgumentException ex) {
        StatusResponseDto restApiException = new StatusResponseDto(
                HttpStatus.BAD_REQUEST.value(),
                ex.getMessage()
        );
        return ResponseEntity.badRequest()
                .body(restApiException);
    }
    // MethodArgumentNotValidException (requestDto에서 valid 관련해서 생기는 예외) 처리
    @ExceptionHandler({MethodArgumentNotValidException.class})
    public ResponseEntity<StatusResponseDto> handleException(MethodArgumentNotValidException ex) {
        StringBuilder sb = new StringBuilder();
        ex.getFieldErrors().forEach((e) -> {
            sb.append(e.getDefaultMessage()).append(" / ");
        });
        sb.setLength(sb.length()-3);
        StatusResponseDto restApiException = new StatusResponseDto(
                HttpStatus.BAD_REQUEST.value(),
                sb.toString()
        );
//
//        StatusCodesResponseDto restApiException = new StatusCodesResponseDto(
//                HttpStatus.BAD_REQUEST.value(),
//                ex.getFieldError().getDefaultMessage());
        return ResponseEntity.badRequest().body(restApiException);
    }

    @ExceptionHandler({NullPointerException.class})
    public ResponseEntity<StatusResponseDto> handleException(NullPointerException ex) {
        StatusResponseDto restApiException = new StatusResponseDto(
                HttpStatus.NOT_FOUND.value(),
                ex.getMessage()
        );
        return new ResponseEntity<>(restApiException, HttpStatus.NOT_FOUND);// notfound에 body값이 들어가지않음
    }

    @ExceptionHandler({AuthorizationServiceException.class})
    public ResponseEntity<StatusResponseDto> handleException(AuthorizationServiceException ex) {
        StatusResponseDto restApiException = new StatusResponseDto(
                HttpStatus.UNAUTHORIZED.value(),
                ex.getMessage()
        );
        return new ResponseEntity<>(restApiException, HttpStatus.UNAUTHORIZED);// unauthorized가 없음...
    }

    @ExceptionHandler({Exception.class})
    public ResponseEntity<StatusResponseDto> handleException(Exception ex) {
        StatusResponseDto restApiException = new StatusResponseDto(
                HttpStatus.BAD_REQUEST.value(),
                ex.getMessage()
        );
        return new ResponseEntity<>(restApiException, HttpStatus.BAD_REQUEST);// unauthorized가 없음...
    }


}