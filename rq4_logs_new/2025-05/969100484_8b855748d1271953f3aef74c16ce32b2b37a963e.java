package com.jia.study_tracker.controller;

import com.jia.study_tracker.exception.InvalidOpenAIResponseException;
import com.jia.study_tracker.exception.OpenAIClientException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(InvalidOpenAIResponseException.class)
    public ResponseEntity<String> handleInvalidResponse(InvalidOpenAIResponseException e) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body("OpenAI 응답이 유효하지 않습니다: " + e.getMessage());
    }


    @ExceptionHandler(OpenAIClientException.class)
    public ResponseEntity<String> handleOpenAIError(OpenAIClientException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body("AI 요약 생성 중 일시적인 서버 오류가 발생했습니다. 잠시 후 다시 시도해주세요.");
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<String> handleGeneral(Exception e) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("예기치 못한 오류가 발생했습니다. 문제가 지속되면 관리자에게 문의해주세요.");
    }
}