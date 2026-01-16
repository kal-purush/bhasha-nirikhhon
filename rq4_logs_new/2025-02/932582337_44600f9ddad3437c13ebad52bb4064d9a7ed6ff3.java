package com.j30n.stoblyx.adapter.in.web;

import com.j30n.stoblyx.common.dto.ApiResponse;
import com.j30n.stoblyx.domain.model.book.exception.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 책 관련 예외를 처리하는 핸들러
 */
@Slf4j
@RestControllerAdvice
@Order(Ordered.HIGHEST_PRECEDENCE)
public class BookExceptionHandler {

    /**
     * 책을 찾을 수 없을 때의 예외 처리
     */
    @ExceptionHandler(BookNotFoundException.class)
    public ResponseEntity<ApiResponse<Void>> handleBookNotFoundException(BookNotFoundException e) {
        log.error("책을 찾을 수 없음: {}", e.getMessage(), e);
        return ResponseEntity
            .status(e.getStatus())
            .body(ApiResponse.error(e.getMessage()));
    }

    /**
     * 책 권한이 없을 때의 예외 처리
     */
    @ExceptionHandler(BookNotAuthorizedException.class)
    public ResponseEntity<ApiResponse<Void>> handleBookNotAuthorizedException(
        BookNotAuthorizedException e
    ) {
        log.error("책 권한 없음: {}", e.getMessage(), e);
        return ResponseEntity
            .status(e.getStatus())
            .body(ApiResponse.error(e.getMessage()));
    }

    /**
     * 이미 존재하는 책을 생성하려 할 때의 예외 처리
     */
    @ExceptionHandler(BookAlreadyExistsException.class)
    public ResponseEntity<ApiResponse<Void>> handleBookAlreadyExistsException(
        BookAlreadyExistsException e
    ) {
        log.error("책 중복 생성 시도: {}", e.getMessage(), e);
        return ResponseEntity
            .status(e.getStatus())
            .body(ApiResponse.error(e.getMessage()));
    }

    /**
     * 기타 책 관련 예외 처리
     */
    @ExceptionHandler(BookException.class)
    public ResponseEntity<ApiResponse<Void>> handleBookException(BookException e) {
        log.error("책 처리 중 오류 발생: {}", e.getMessage(), e);
        return ResponseEntity
            .status(e.getStatus())
            .body(ApiResponse.error(e.getMessage()));
    }

    /**
     * 책 유효성 검사 예외 처리
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiResponse<Void>> handleIllegalArgumentException(IllegalArgumentException e) {
        log.error("책 유효성 검사 오류: {}", e.getMessage(), e);
        return ResponseEntity.badRequest()
            .body(ApiResponse.error(e.getMessage()));
    }
} 